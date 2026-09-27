//! Per-request bounds on `/graphql` (docs/architecture.md §72): body size,
//! query depth, query complexity, and how many expensive list fields one
//! request may select. Without them a single request could be arbitrarily
//! large (read whole, before authentication), arbitrarily deep, or alias
//! one paged field hundreds of times - each alias getting its own full
//! `max_events_per_read` page.

use async_graphql::extensions::{Extension, ExtensionContext, ExtensionFactory, NextParseQuery};
use async_graphql::parser::types::{ExecutableDocument, Selection, SelectionSet};
use async_graphql::{ServerError, ServerResult, Variables};
use std::sync::Arc;

/// `skilj::SkiljBuilder::graphql_limits`. The defaults fit every query
/// this workspace's own tools send, including the standard full
/// introspection query GraphiQL and codegen tools use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphqlLimits {
    /// Largest accepted `POST /graphql` body, in bytes - anything larger
    /// is `413 Payload Too Large`, checked before authentication or
    /// parsing - and largest websocket message/frame, beyond which the
    /// subscription connection is closed. Default 2 MiB, axum's own
    /// default for a JSON body.
    pub max_request_body_bytes: usize,
    /// Deepest selection nesting a query may have. Default 24; the full
    /// introspection query needs about 14.
    pub max_depth: usize,
    /// Most fields (counting nested ones) a query may select. Default 2000.
    pub max_complexity: usize,
    /// Most occurrences of [`EXPENSIVE_FIELDS`] one operation may select,
    /// aliases and fragments included - each serves a full page (or, for
    /// `countEvents`, scans history) on its own. Default 10.
    pub max_expensive_fields: usize,
    /// Most subscriptions one websocket connection may have running at
    /// once; another is refused with `too_many_subscriptions` until one
    /// ends. Each holds its own event-broadcast receiver and filters
    /// every committed event. Default 100.
    pub max_subscriptions_per_connection: usize,
}

impl Default for GraphqlLimits {
    fn default() -> Self {
        Self {
            max_request_body_bytes: 2 * 1024 * 1024,
            max_depth: 24,
            max_complexity: 2000,
            max_expensive_fields: 10,
            max_subscriptions_per_connection: 100,
        }
    }
}

/// The fields whose cost scales with stored history rather than with the
/// query itself.
pub const EXPENSIVE_FIELDS: &[&str] =
    &["queryEvents", "countEvents", "fetchCommands", "projection"];

/// Refuses an operation selecting more than `max` [`EXPENSIVE_FIELDS`].
/// Checked on the parsed document, before validation and execution.
pub(crate) struct ExpensiveFieldBudget {
    pub(crate) max: usize,
}

impl ExtensionFactory for ExpensiveFieldBudget {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(ExpensiveFieldBudgetExtension { max: self.max })
    }
}

struct ExpensiveFieldBudgetExtension {
    max: usize,
}

#[async_graphql::async_trait::async_trait]
impl Extension for ExpensiveFieldBudgetExtension {
    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Variables,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        let document = next.run(ctx, query, variables).await?;
        // Only one operation executes, so the most expensive one decides.
        let worst = document
            .operations
            .iter()
            .map(|(_, op)| count_expensive(&document, &op.node.selection_set.node, &mut Vec::new()))
            .max()
            .unwrap_or(0);
        if worst > self.max {
            let mut error = ServerError::new(
                format!(
                    "this query selects {worst} of {EXPENSIVE_FIELDS:?} (aliases and fragments \
                     included); at most {} are allowed per request",
                    self.max
                ),
                None,
            );
            error.extensions = Some({
                let mut extensions = async_graphql::ErrorExtensionValues::default();
                extensions.set("code", "query_too_expensive");
                extensions
            });
            return Err(error);
        }
        Ok(document)
    }
}

/// Counts [`EXPENSIVE_FIELDS`] selections under `selection_set`,
/// expanding fragment spreads. `expanding` holds the fragments currently
/// being expanded: this runs before validation, which is what would
/// otherwise reject a fragment cycle, so a cycle is simply not followed.
fn count_expensive<'a>(
    document: &'a ExecutableDocument,
    selection_set: &'a SelectionSet,
    expanding: &mut Vec<&'a str>,
) -> usize {
    selection_set
        .items
        .iter()
        .map(|selection| match &selection.node {
            Selection::Field(field) => {
                usize::from(EXPENSIVE_FIELDS.contains(&field.node.name.node.as_str()))
                    + count_expensive(document, &field.node.selection_set.node, expanding)
            }
            Selection::InlineFragment(fragment) => {
                count_expensive(document, &fragment.node.selection_set.node, expanding)
            }
            Selection::FragmentSpread(spread) => {
                let name = spread.node.fragment_name.node.as_str();
                match document.fragments.get(name) {
                    Some(fragment) if !expanding.contains(&name) => {
                        expanding.push(name);
                        let count =
                            count_expensive(document, &fragment.node.selection_set.node, expanding);
                        expanding.pop();
                        count
                    }
                    _ => 0,
                }
            }
        })
        .sum()
}

/// One websocket connection's count of running subscriptions - inserted
/// into the connection's data at `connection_init`, so every subscription
/// resolver on that connection sees the same counter.
#[derive(Clone, Default)]
pub(crate) struct ConnectionSubscriptions(Arc<std::sync::atomic::AtomicUsize>);

/// One running subscription's claim on its connection's
/// [`ConnectionSubscriptions`], released on drop - owned by the
/// subscription's stream, so it goes when the stream does, however that
/// happens (client `complete`, the stream ending, the connection closing).
pub(crate) struct SubscriptionSlot(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for SubscriptionSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Claims a subscription slot on this resolver's connection, or refuses
/// with `too_many_subscriptions` when `max` are already running. `None`
/// outside a websocket connection (no [`ConnectionSubscriptions`] in the
/// data - e.g. a schema executed directly), where there's no connection
/// to bound.
pub(crate) fn acquire_subscription_slot(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    max: usize,
) -> async_graphql::Result<Option<SubscriptionSlot>> {
    use async_graphql::ErrorExtensions;
    use std::sync::atomic::Ordering;

    let Ok(connection) = ctx.data::<ConnectionSubscriptions>() else {
        return Ok(None);
    };
    connection
        .0
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |running| {
            (running < max).then_some(running + 1)
        })
        .map_err(|running| {
            async_graphql::Error::new(format!(
                "this connection already has {running} subscriptions running; at most {max} \
                 are allowed - complete one first"
            ))
            .extend_with(|_, ext| ext.set("code", "too_many_subscriptions"))
        })?;
    Ok(Some(SubscriptionSlot(connection.0.clone())))
}

/// `POST /graphql`'s body cap: reads at most `max` bytes (so a missing or
/// false `Content-Length`, or a chunked body, can't get past it) and
/// answers `413` beyond that - before authentication, parsing or anything
/// else. The GraphQL extractor buffered the whole body anyway; this only
/// bounds it. The websocket upgrade (`GET`) is left alone.
pub(crate) async fn limit_request_body(
    max: usize,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    if request.method() != axum::http::Method::POST {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    match axum::body::to_bytes(body, max).await {
        Ok(bytes) => {
            next.run(axum::extract::Request::from_parts(
                parts,
                axum::body::Body::from(bytes),
            ))
            .await
        }
        Err(_) => (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            format!("request body exceeds {max} bytes"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worst(query: &str) -> usize {
        let document = async_graphql::parser::parse_query(query).unwrap();
        document
            .operations
            .iter()
            .map(|(_, op)| count_expensive(&document, &op.node.selection_set.node, &mut Vec::new()))
            .max()
            .unwrap_or(0)
    }

    #[test]
    fn aliases_and_fragment_spreads_each_count() {
        assert_eq!(
            worst("{ a: countEvents b: countEvents boundedContexts }"),
            2
        );
        assert_eq!(
            worst("{ ...F ...F } fragment F on Query { queryEvents fetchCommands }"),
            4
        );
        assert_eq!(worst("{ ... on Query { projection } }"), 1);
    }

    #[test]
    fn the_most_expensive_operation_decides() {
        assert_eq!(
            worst("query A { countEvents } query B { a: countEvents b: countEvents }"),
            2
        );
    }

    /// Runs before validation, which is what rejects fragment cycles - so
    /// the walk itself must not follow one forever.
    #[test]
    fn a_fragment_cycle_is_not_followed() {
        assert_eq!(
            worst("{ ...A } fragment A on Query { countEvents ...B } fragment B on Query { ...A }"),
            1
        );
    }
}
