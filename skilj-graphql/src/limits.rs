//! Per-request bounds on `/graphql` (docs/architecture.md §72): body size,
//! query depth, query complexity, and how many expensive list fields one
//! request may select. Without them a single request could be arbitrarily
//! large (read whole, before authentication), arbitrarily deep, or alias
//! one paged field hundreds of times - each alias getting its own full
//! `max_events_per_read` page.

use async_graphql::extensions::{
    Extension, ExtensionContext, ExtensionFactory, NextPrepareRequest,
};
use async_graphql::parser::types::{ExecutableDocument, Selection, SelectionSet};
use async_graphql::{Request, ServerError, ServerResult};
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
    /// How long a websocket may stay open without sending
    /// `connection_init`; after that it is closed with 4408, graphql-ws's
    /// "Connection initialisation timeout". Default 10 seconds
    /// (docs/architecture.md §136).
    pub websocket_init_timeout: std::time::Duration,
    /// How often the server sends a websocket ping frame. A connection
    /// from which nothing - not even the pong every websocket client
    /// answers a ping with automatically - has arrived for two intervals
    /// is treated as dead and dropped, freeing its subscriptions. `None`
    /// sends no pings and never drops a quiet connection. Default 30
    /// seconds (docs/architecture.md §136).
    pub websocket_ping_interval: Option<std::time::Duration>,
}

impl Default for GraphqlLimits {
    fn default() -> Self {
        Self {
            max_request_body_bytes: 2 * 1024 * 1024,
            max_depth: 24,
            max_complexity: 2000,
            max_expensive_fields: 10,
            max_subscriptions_per_connection: 100,
            websocket_init_timeout: std::time::Duration::from_secs(10),
            websocket_ping_interval: Some(std::time::Duration::from_secs(30)),
        }
    }
}

/// The fields whose cost scales with stored history rather than with the
/// query itself.
pub const EXPENSIVE_FIELDS: &[&str] = &[
    "queryEvents",
    "searchUsedTags",
    "countEvents",
    "fetchCommands",
    "projection",
    "parkedDeliveries",
    "commandProposals",
];

/// A hard bound on fragment-expanded work before async-graphql's own
/// recursive depth and validation passes. This includes ordinary fields,
/// fragment spreads and every operation, even operations not selected.
const MAX_EXPANDED_SELECTIONS: usize = 10_000;
const MAX_RECURSIVE_DEPTH: usize = 32;

/// Checks document work and expensive fields before the dependency's
/// recursive parse/validation passes. Installed on every served schema.
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
    async fn prepare_request(
        &self,
        ctx: &ExtensionContext<'_>,
        mut request: Request,
        next: NextPrepareRequest<'_>,
    ) -> ServerResult<Request> {
        // parsed_query parses and caches the syntax tree without running
        // async-graphql's recursive fragment traversal. parse_query hooks
        // are too late: NextParseQuery already performs that traversal.
        let document = request.parsed_query()?;
        let mut remaining = MAX_EXPANDED_SELECTIONS;
        for (_, operation) in document.operations.iter() {
            let count =
                count_expensive(document, &operation.node.selection_set.node, &mut remaining)?;
            if count > self.max {
                return Err(budget_error("query_too_expensive", format!(
                    "this query selects {count} expensive fields; at most {} are allowed per request",
                    self.max
                )));
            }
        }
        // Validation also visits definitions that no operation selects.
        for fragment in document.fragments.values() {
            count_expensive(document, &fragment.node.selection_set.node, &mut remaining)?;
        }
        next.run(ctx, request).await
    }
}

fn budget_error(code: &str, message: String) -> ServerError {
    let mut error = ServerError::new(message, None);
    let mut extensions = async_graphql::ErrorExtensionValues::default();
    extensions.set("code", code);
    error.extensions = Some(extensions);
    error
}

/// An iterative, bounded expansion: at most MAX_EXPANDED_SELECTIONS
/// selections are visited across the entire document. The depth bound
/// rejects cycles and long fragment chains without recursive Rust calls.
/// Limiting all selections, including zero-cost leaves, is essential.
fn count_expensive(
    document: &ExecutableDocument,
    selection_set: &SelectionSet,
    remaining: &mut usize,
) -> ServerResult<usize> {
    let mut count = 0;
    let mut stack = vec![(selection_set, 0)];
    while let Some((set, depth)) = stack.pop() {
        if depth > MAX_RECURSIVE_DEPTH {
            return Err(budget_error(
                "query_too_complex",
                "query fragment depth exceeds the document budget".into(),
            ));
        }
        for selection in &set.items {
            *remaining = remaining.checked_sub(1).ok_or_else(|| {
                budget_error(
                    "query_too_complex",
                    "query fragment expansion exceeds the document budget".into(),
                )
            })?;
            match &selection.node {
                Selection::Field(field) => {
                    count += usize::from(EXPENSIVE_FIELDS.contains(&field.node.name.node.as_str()));
                    if !field.node.selection_set.node.items.is_empty() {
                        stack.push((&field.node.selection_set.node, depth + 1));
                    }
                }
                Selection::InlineFragment(fragment) => {
                    stack.push((&fragment.node.selection_set.node, depth + 1));
                }
                Selection::FragmentSpread(spread) => {
                    if let Some(fragment) = document
                        .fragments
                        .get(spread.node.fragment_name.node.as_str())
                    {
                        stack.push((&fragment.node.selection_set.node, depth + 1));
                    }
                }
            }
        }
    }
    Ok(count)
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
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |running| {
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
            .map(|(_, op)| {
                let mut remaining = MAX_EXPANDED_SELECTIONS;
                count_expensive(&document, &op.node.selection_set.node, &mut remaining).unwrap()
            })
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

    #[test]
    fn cycles_fail_closed_with_bounded_work() {
        let document = async_graphql::parser::parse_query(
            "{ ...A } fragment A on Query { ...B } fragment B on Query { ...A }",
        )
        .unwrap();
        let operation = document.operations.iter().next().unwrap().1;
        let mut remaining = MAX_EXPANDED_SELECTIONS;
        assert!(count_expensive(
            &document,
            &operation.node.selection_set.node,
            &mut remaining
        )
        .is_err());
    }

    fn fragment_bomb(leaf: &str) -> String {
        let mut query = "{ ...F0 }\n".to_string();
        for i in 0..20 {
            query.push_str(&format!("fragment F{i} on Query {{ "));
            if i == 19 {
                query.push_str(leaf);
            } else {
                for _ in 0..6 {
                    query.push_str(&format!("...F{} ", i + 1));
                }
            }
            query.push_str(" }\n");
        }
        query
    }

    #[tokio::test]
    async fn fragment_bombs_are_refused_before_dependency_traversal() {
        use async_graphql::dynamic::{
            Field, FieldFuture, FieldValue, Object, Schema, Subscription, SubscriptionField,
            SubscriptionFieldFuture, TypeRef,
        };
        let query = Object::new("Query").field(Field::new(
            "ok",
            TypeRef::named_nn(TypeRef::STRING),
            |_| FieldFuture::new(async { Ok(Some(FieldValue::value("ok"))) }),
        ));
        let subscription = Subscription::new("Subscription").field(SubscriptionField::new(
            "ok",
            TypeRef::named_nn(TypeRef::STRING),
            |_| {
                SubscriptionFieldFuture::new(async {
                    Ok(async_graphql::futures_util::stream::empty::<
                        async_graphql::Result<FieldValue<'static>>,
                    >())
                })
            },
        ));
        let schema = Schema::build("Query", None, Some("Subscription"))
            .register(query)
            .register(subscription)
            .limit_depth(24)
            .limit_complexity(2000)
            .extension(ExpensiveFieldBudget { max: 10 })
            .finish()
            .unwrap();
        // A regular scalar, an expensive field, an unknown field, an
        // unselected operation and the subscription execution path.
        for leaf in ["ok", "queryEvents", "unknown"] {
            let query = fragment_bomb(leaf);
            let response = schema.execute(&query).await;
            assert_eq!(
                response.errors[0].extensions.as_ref().unwrap().get("code"),
                Some(&async_graphql::Value::from("query_too_complex"))
            );
            let query = query.replacen("{ ...F0 }", "query Safe { ok } query Bomb { ...F0 }", 1);
            let response = schema
                .execute(Request::new(query).operation_name("Safe"))
                .await;
            assert_eq!(
                response.errors[0].extensions.as_ref().unwrap().get("code"),
                Some(&async_graphql::Value::from("query_too_complex"))
            );
        }
        let unused = fragment_bomb("ok").replacen("{ ...F0 }", "{ ok }", 1);
        let response = schema.execute(unused).await;
        assert_eq!(
            response.errors[0].extensions.as_ref().unwrap().get("code"),
            Some(&async_graphql::Value::from("query_too_complex"))
        );
        let service =
            crate::federation::service_schema(Arc::new("type Query { ok: String }".into()));
        let response = service.execute(fragment_bomb("__typename")).await;
        assert_eq!(
            response.errors[0].extensions.as_ref().unwrap().get("code"),
            Some(&async_graphql::Value::from("query_too_complex"))
        );
        use async_graphql::futures_util::StreamExt;
        let response = schema
            .execute_stream(
                fragment_bomb("ok")
                    .replacen("{ ...F0 }", "subscription { ...F0 }", 1)
                    .replace("on Query", "on Subscription"),
            )
            .next()
            .await
            .unwrap();
        assert_eq!(
            response.errors[0].extensions.as_ref().unwrap().get("code"),
            Some(&async_graphql::Value::from("query_too_complex"))
        );
        assert!(schema.execute("{ ok }").await.errors.is_empty());
    }
}
