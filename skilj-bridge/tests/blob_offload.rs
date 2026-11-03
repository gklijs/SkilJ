//! What a bridge does with a mapping's blob fields (issue #132,
//! docs/architecture.md §239), against a stub of skilj's upload route: each
//! marked value is uploaded whatever its size and replaced by the reference
//! skilj answers, the rest of the message stays, and a mapping with blobs
//! never goes through the batch routes.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use skilj_bridge::{
    offload_blob_fields, post_inbound_batch, too_large_refusal, InboundAction, InboundBatcher,
    InboundBatching, InboundBlobField, InboundBlobs, InboundItem, InboundMapping,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// One upload the stub saw: bounded context, media type, query, bytes.
type Seen = Arc<Mutex<Vec<(String, String, HashMap<String, String>, Vec<u8>)>>>;

async fn upload(
    State(seen): State<Seen>,
    Path(bounded_context): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let media_type = headers["content-type"].to_str().unwrap().to_string();
    let mut seen = seen.lock().unwrap();
    seen.push((bounded_context, media_type.clone(), query, body.to_vec()));
    let id = format!("blob-{}", seen.len());
    (
        StatusCode::CREATED,
        Json(json!({"$blob": id, "sha256": "ab", "size": body.len(), "media_type": media_type})),
    )
}

async fn stub() -> (String, Seen) {
    let seen = Seen::default();
    let app = Router::new()
        .route("/v1/blobs/{bounded_context}", post(upload))
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, seen)
}

fn mapping(fields: Vec<InboundBlobField>) -> InboundMapping {
    InboundMapping {
        blobs: Some(InboundBlobs {
            bounded_context: "files".to_string(),
            fields,
        }),
        credential: "id.secret".to_string(),
        action: InboundAction::Record {
            event_type: "Filed".to_string(),
        },
    }
}

fn field(name: &str, subject: Option<(&str, &str)>) -> InboundBlobField {
    InboundBlobField {
        field: name.to_string(),
        media_type: "text/plain".to_string(),
        subject: subject.map(|(key, field)| (key.to_string(), field.to_string())),
    }
}

#[tokio::test]
async fn a_marked_field_becomes_a_reference_and_the_rest_stays() {
    let (url, seen) = stub().await;
    let mapping = mapping(vec![field("doc.body", Some(("customer", "customer_id")))]);
    let message = json!({"customer_id": "c1", "doc": {"body": "x", "title": "t"}}).to_string();
    let http = reqwest::Client::new();
    let out = offload_blob_fields(&http, &url, &mapping, message.as_bytes())
        .await
        .unwrap()
        .expect("replaced");
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["doc"]["body"]["$blob"], "blob-1");
    assert_eq!(out["doc"]["title"], "t");
    assert_eq!(out["customer_id"], "c1");
    let seen = seen.lock().unwrap();
    // A one-byte value is uploaded all the same: one shape, whatever its size.
    assert_eq!(seen[0].3, b"x");
    assert_eq!(seen[0].0, "files");
    assert_eq!(seen[0].1, "text/plain");
    assert_eq!(seen[0].2["subjectKey"], "customer");
    assert_eq!(seen[0].2["subjectValue"], "c1");
}

#[tokio::test]
async fn a_list_has_each_element_uploaded_and_other_json_as_its_text() {
    let (url, seen) = stub().await;
    let mapping = mapping(vec![field("items", None)]);
    let message = json!({"items": ["a", {"k": 1}]}).to_string();
    let http = reqwest::Client::new();
    let out = offload_blob_fields(&http, &url, &mapping, message.as_bytes())
        .await
        .unwrap()
        .expect("replaced");
    let out: Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(out["items"][1]["$blob"], "blob-2");
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].3, b"a");
    assert_eq!(seen[1].3, br#"{"k":1}"#);
    assert!(seen[0].2.is_empty());
}

#[tokio::test]
async fn nothing_to_replace_answers_none() {
    let (url, seen) = stub().await;
    let http = reqwest::Client::new();
    let marked = mapping(vec![field("doc", None)]);
    let absent = offload_blob_fields(&http, &url, &marked, br#"{"doc":null,"x":1}"#);
    assert_eq!(absent.await.unwrap(), None);
    let not_json = offload_blob_fields(&http, &url, &marked, b"not json");
    assert_eq!(not_json.await.unwrap(), None);
    let mut none = mapping(Vec::new());
    none.blobs = None;
    let unmapped = offload_blob_fields(&http, &url, &none, br#"{"doc":"x"}"#);
    assert_eq!(unmapped.await.unwrap(), None);
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_mapping_with_blobs_never_batches() {
    let http = reqwest::Client::new();
    let marked = mapping(vec![field("doc", None)]);
    let body = json!({"payload": {}});
    let items = [
        InboundItem {
            body: &body,
            idempotency_key: None,
        },
        InboundItem {
            body: &body,
            idempotency_key: None,
        },
    ];
    let batcher = InboundBatcher::new(InboundBatching::off());
    // Unreachable URL: it answers 0 without calling anything.
    let done = post_inbound_batch(&http, "http://127.0.0.1:1", &marked, &batcher, &items).await;
    assert_eq!(done, 0);
}

#[test]
fn only_a_size_refusal_counts_as_too_large() {
    assert!(too_large_refusal(r#"{"code":"payload_too_large"}"#));
    assert!(too_large_refusal(r#"{"code":"blob_too_large"}"#));
    assert!(!too_large_refusal(r#"{"code":"blob_reference_invalid"}"#));
    assert!(!too_large_refusal("not json"));
}
