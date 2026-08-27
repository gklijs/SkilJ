//! Reading an arbitrary registered `Projection`'s state generically -
//! harder than it looks, and worth its own module rather than a plain
//! query string in `app.rs`. `projection(...): ProjectionResult!` is a
//! GraphQL **union**, one concrete member type per registered
//! projection, generated at runtime from that projection's own JSON
//! Schema (`skilj-graphql::projection_types::build`) - there's no
//! generic `{ state: String }` shape to ask for, and this crate (by
//! design - see its own doc comment) has no prior knowledge of any
//! bounded context's schema to hand-write a selection set against.
//!
//! Resolved with two round trips, driven entirely by the wire protocol
//! itself (GraphQL introspection), never by replicating the server's own
//! internal naming scheme:
//!
//! 1. Ask for just `__typename` - the server always answers with which
//!    concrete union member this particular result actually is.
//! 2. Introspect *that* type's own fields (`__type(name: ...)`), build a
//!    selection set from them (recursing one level into any nested
//!    `OBJECT` fields - real projection shapes here are flat or close to
//!    it, so a small, bounded recursion depth is enough; deeply nested
//!    or self-referential projection shapes are a known v1 limitation,
//!    not silently mishandled), and re-run the query for real.

use crate::graphql::{Client, ClientError};
use serde_json::{json, Value};

const MAX_RECURSION_DEPTH: usize = 3;

const INTROSPECT_TYPE_QUERY: &str = "query($name: String!) { \
    __type(name: $name) { \
        fields { \
            name \
            type { \
                kind name \
                ofType { kind name ofType { kind name ofType { kind name } } } \
            } \
        } \
    } \
}";

/// Codeberg issue #7's own naming honesty note: `wait_for_sequence` is a
/// *freshness* guarantee - "don't answer before the projection has
/// caught up to at least this sequence" - not a historical snapshot. If
/// the projection has already moved past it, the caller gets *current*
/// state, not state frozen at that instant (see `ProjectionsField`'s own
/// UI label, which says "at least as fresh as", never "as of", for
/// exactly this reason).
pub async fn fetch(
    client: &Client,
    bounded_context: &str,
    name: &str,
    key: Option<&str>,
    wait_for_sequence: Option<i64>,
) -> Result<Value, ClientError> {
    let key_value = key.map(|k| Value::String(k.to_string())).unwrap_or(Value::Null);
    let wait_value = wait_for_sequence.map(Value::from).unwrap_or(Value::Null);
    let variables =
        json!({ "bc": bounded_context, "name": name, "key": key_value, "wait": wait_value });

    let typename_response = client
        .request(
            "query($bc: String!, $name: String!, $key: String, $wait: Int) { \
                projection(boundedContext: $bc, name: $name, key: $key, waitForSequence: $wait) { \
                    __typename \
                } \
            }",
            variables.clone(),
        )
        .await?;
    let type_name = typename_response
        .pointer("/projection/__typename")
        .and_then(Value::as_str)
        .ok_or_else(|| ClientError::MalformedResponse("projection result had no __typename".into()))?
        .to_string();

    let selection = build_selection(client, &type_name, 0).await?;
    if selection.is_empty() {
        return Ok(typename_response);
    }

    let query = format!(
        "query($bc: String!, $name: String!, $key: String, $wait: Int) {{ \
            projection(boundedContext: $bc, name: $name, key: $key, waitForSequence: $wait) {{ \
                __typename ... on {type_name} {{ {selection} }} \
            }} \
        }}"
    );
    client.request(&query, variables).await
}

/// Builds a GraphQL selection set string for every field of `type_name`,
/// via introspection - bare `fieldName` for a scalar/enum leaf,
/// `fieldName { ... }` for a nested `OBJECT` (recursing, capped at
/// `MAX_RECURSION_DEPTH` so a pathological or self-referential shape
/// can't recurse forever). `Box::pin` on the recursive call - an `async
/// fn` calling itself needs an indirection somewhere to have a known
/// size.
fn build_selection<'a>(
    client: &'a Client,
    type_name: &'a str,
    depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ClientError>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_RECURSION_DEPTH {
            return Ok(String::new());
        }
        let response = client
            .request(INTROSPECT_TYPE_QUERY, json!({ "name": type_name }))
            .await?;
        let fields = response
            .pointer("/__type/fields")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let mut parts = Vec::new();
        for field in &fields {
            let Some(field_name) = field.get("name").and_then(Value::as_str) else {
                continue;
            };
            let (kind, named_type) = unwrap_type(field.get("type").unwrap_or(&Value::Null));
            if kind == "OBJECT" {
                let inner = build_selection(client, &named_type, depth + 1).await?;
                if !inner.is_empty() {
                    parts.push(format!("{field_name} {{ {inner} }}"));
                }
                // An OBJECT field whose own introspection came back empty
                // (recursion cap, or a type with no fields) is dropped
                // entirely - asking for `field { }` is invalid GraphQL.
            } else {
                parts.push(field_name.to_string());
            }
        }
        Ok(parts.join(" "))
    })
}

/// Walks a `__Type` introspection value's `NON_NULL`/`LIST` wrappers
/// down to the named underlying type - `(kind, name)`, e.g. `("SCALAR",
/// "String")` or `("OBJECT", "banking_AccountBalance")`.
fn unwrap_type(type_value: &Value) -> (String, String) {
    let mut current = type_value;
    loop {
        let kind = current.get("kind").and_then(Value::as_str).unwrap_or("").to_string();
        if kind == "NON_NULL" || kind == "LIST" {
            current = current.get("ofType").unwrap_or(&Value::Null);
            continue;
        }
        let name = current.get("name").and_then(Value::as_str).unwrap_or("").to_string();
        return (kind, name);
    }
}
