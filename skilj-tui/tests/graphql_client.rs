//! Verifies `graphql::Client::request` end to end against a small local
//! mock HTTP server: the request it actually sends (method, bearer auth,
//! `{query, variables}` body shape) and how it parses a response back
//! (`data` on success, `errors[]` - including the `code`/`traceId`
//! extensions - into a typed `ClientError::Graphql` on failure).

use serde_json::{json, Value};
use skilj_tui::graphql::{Client, ClientError};
use std::sync::{Arc, Mutex};

async fn serve(response_body: Value, captured: Arc<Mutex<Option<Value>>>) -> reqwest::Url {
    let app = axum::Router::new().route(
        "/graphql",
        axum::routing::post(
            move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<Value>| {
                let response_body = response_body.clone();
                let captured = captured.clone();
                async move {
                    *captured.lock().unwrap() = Some(json!({
                        "body": body,
                        "authorization": headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string),
                    }));
                    axum::Json(response_body)
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}/graphql").parse().unwrap()
}

#[tokio::test]
async fn sends_the_bearer_token_and_the_query_variables_shape() {
    let captured = Arc::new(Mutex::new(None));
    let endpoint = serve(json!({ "data": { "ok": true } }), captured.clone()).await;
    let client = Client::new(endpoint, "test-jwt".to_string());

    let data = client
        .request("query($x: Int!) { thing(x: $x) }", json!({ "x": 1 }))
        .await
        .expect("a well-formed data response should parse as Ok");

    assert_eq!(data, json!({ "ok": true }));
    let captured = captured.lock().unwrap().clone().expect("the server should have captured one request");
    assert_eq!(captured["authorization"], "Bearer test-jwt");
    assert_eq!(captured["body"]["query"], "query($x: Int!) { thing(x: $x) }");
    assert_eq!(captured["body"]["variables"], json!({ "x": 1 }));
}

#[tokio::test]
async fn graphql_errors_carry_their_code_and_trace_id() {
    let captured = Arc::new(Mutex::new(None));
    let endpoint = serve(
        json!({
            "errors": [{
                "message": "no active superadmin",
                "extensions": { "code": "not_superadmin", "traceId": "abc123" },
            }]
        }),
        captured,
    )
    .await;
    let client = Client::new(endpoint, "test-jwt".to_string());

    let error = client
        .request("query { boundedContexts { name } }", Value::Null)
        .await
        .expect_err("a response with a non-empty errors[] should be Err");

    let ClientError::Graphql(errors) = error else {
        panic!("expected ClientError::Graphql, got {error:?}");
    };
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].message, "no active superadmin");
    assert_eq!(errors[0].code.as_deref(), Some("not_superadmin"));
    assert_eq!(errors[0].trace_id.as_deref(), Some("abc123"));
}

#[tokio::test]
async fn a_response_with_neither_data_nor_errors_is_a_malformed_response_error() {
    let captured = Arc::new(Mutex::new(None));
    let endpoint = serve(json!({}), captured).await;
    let client = Client::new(endpoint, "test-jwt".to_string());

    let error = client
        .request("query { x }", Value::Null)
        .await
        .expect_err("a response with neither data nor errors should be Err");

    assert!(matches!(error, ClientError::MalformedResponse(_)), "got {error:?}");
}
