//! Verifies `projection_query::fetch`'s introspection-driven flow end to
//! end against a small local mock server: resolve `__typename` for the
//! union instance, introspect that concrete type's fields (recursing one
//! level into a nested `OBJECT` field), build a selection set from them,
//! then re-fetch with it - all without this crate ever having prior
//! knowledge of the projection's shape, matching how a real
//! `skilj-graphql` deployment's dynamically-generated per-projection
//! types work (docs/architecture.md §11 / §5.1).

use serde_json::{json, Value};
use skilj_tui::graphql::Client;
use skilj_tui::projection_query::fetch;

async fn serve_mock_projection_server() -> reqwest::Url {
    let app = axum::Router::new().route(
        "/graphql",
        axum::routing::post(|axum::Json(body): axum::Json<Value>| async move {
            axum::Json(respond_to(&body))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}/graphql").parse().unwrap()
}

fn respond_to(body: &Value) -> Value {
    let query = body["query"].as_str().unwrap_or_default();

    if query.contains("__typename") && !query.contains("... on") {
        // Step 1: resolve which concrete union member this instance is.
        return json!({ "data": { "projection": { "__typename": "banking_AccountBalance" } } });
    }

    if query.contains("__type(name:") {
        // Step 2: introspect whichever type was asked for.
        let type_name = body["variables"]["name"].as_str().unwrap_or_default();
        let fields = match type_name {
            "banking_AccountBalance" => json!([
                { "name": "balance", "type": { "kind": "NON_NULL", "name": null, "ofType": { "kind": "SCALAR", "name": "Int", "ofType": null } } },
                { "name": "owner", "type": { "kind": "OBJECT", "name": "banking_Owner", "ofType": null } },
            ]),
            "banking_Owner" => json!([
                { "name": "name", "type": { "kind": "SCALAR", "name": "String", "ofType": null } },
            ]),
            other => panic!("test only stubs introspection for banking_AccountBalance/banking_Owner, got {other:?}"),
        };
        return json!({ "data": { "__type": { "fields": fields } } });
    }

    if query.contains("... on banking_AccountBalance") {
        // Step 3: the real, selection-set-driven fetch.
        assert!(query.contains("balance"), "selection set should include the scalar field: {query}");
        assert!(
            query.contains("owner { name }"),
            "selection set should recurse into the nested OBJECT field: {query}"
        );
        return json!({
            "data": {
                "projection": {
                    "__typename": "banking_AccountBalance",
                    "balance": 4200,
                    "owner": { "name": "a1" },
                }
            }
        });
    }

    panic!("unexpected query in test: {query}");
}

#[tokio::test]
async fn resolves_an_arbitrary_projections_shape_via_introspection_and_fetches_it() {
    let endpoint = serve_mock_projection_server().await;
    let client = Client::new(endpoint, "test-jwt".to_string());

    let result = fetch(&client, "banking", "AccountBalance", Some("a1"))
        .await
        .expect("the full introspect-then-fetch flow should succeed");

    assert_eq!(
        result,
        json!({
            "projection": {
                "__typename": "banking_AccountBalance",
                "balance": 4200,
                "owner": { "name": "a1" },
            }
        })
    );
}
