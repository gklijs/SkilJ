#[path = "common.rs"]
mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use skilj_core::model::AccessLevel;
use skilj_server::auth::{JwtConfig, KeySource, Verifier};
use tower::ServiceExt;

const SECRET: &[u8] = b"test-idp-secret";

fn jwt(sub: &str) -> String {
    let claims = json!({"sub": sub, "iss": "https://idp.test", "aud": "skilj", "exp": chrono::Utc::now().timestamp() + 3600});
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(SECRET),
    )
    .unwrap()
}

fn verifier() -> Verifier {
    Verifier::new(JwtConfig {
        issuer: "https://idp.test".into(),
        audience: "skilj".into(),
        algorithm: Algorithm::HS256,
        subject_claim: "sub".into(),
        keys: KeySource::Secret(SECRET.to_vec()),
    })
}

async fn call(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn gql(app: &axum::Router, sub: Option<&str>, query: &str, vars: Value) -> Value {
    let mut b = Request::post("/graphql").header("content-type", "application/json");
    if let Some(s) = sub {
        b = b.header("authorization", format!("Bearer {}", jwt(s)));
    }
    let (_, v) = call(
        app,
        b.body(Body::from(
            json!({"query": query, "variables": vars}).to_string(),
        ))
        .unwrap(),
    )
    .await;
    v
}

#[tokio::test]
async fn graphql_and_rest_end_to_end() {
    let env = common::start("http_e2e").await;
    let s = env.skilj.clone();
    let writer = env
        .role("writer@idp", AccessLevel::Write, false, None)
        .await;
    let _ = writer;
    let app = skilj_server::router(s.clone(), verifier());

    // Unknown context is invisible; a granted one answers.
    let v = gql(
        &app,
        Some("writer@idp"),
        "{ admin { countEvents } }",
        json!({}),
    )
    .await;
    assert_eq!(v["errors"][0]["extensions"]["code"], "NOT_FOUND", "{v}");

    // Typed command mutation.
    let v = gql(
        &app,
        Some("writer@idp"),
        r#"mutation($i: BankOpenAccountInput!) { bank_openAccount(input: $i, idempotencyKey: "k1") { status commandId highestSequence events { sequence type } } }"#,
        json!({"i": {"account_id": "a1", "owner_id": "u1", "company": "acme"}}),
    )
    .await;
    assert_eq!(v["data"]["bank_openAccount"]["status"], "ACCEPTED", "{v}");
    let v = gql(
        &app,
        Some("writer@idp"),
        r#"mutation { bank_submit(type: "Deposit", payload: {account_id: "a1", amount: 12}) { status highestSequence } }"#,
        json!({}),
    )
    .await;
    assert_eq!(v["data"]["bank_submit"]["status"], "ACCEPTED", "{v}");
    let seq = v["data"]["bank_submit"]["highestSequence"]
        .as_i64()
        .unwrap();
    let v = gql(
        &app,
        Some("writer@idp"),
        r#"mutation { bank_submit(type: "Withdraw", payload: {account_id: "a1", amount: 99}) { status rejectionKind matchingEvents { sequence } } }"#,
        json!({}),
    )
    .await;
    assert_eq!(
        v["data"]["bank_submit"]["rejectionKind"], "insufficient_balance",
        "{v}"
    );
    assert!(v["data"]["bank_submit"]["matchingEvents"].is_null());

    // Typed projection with read-your-writes (sync projection, async one).
    s.tick().await.unwrap();
    let v = gql(
        &app,
        Some("writer@idp"),
        &format!(r#"{{ bank {{ Balance(key: "a1", waitForSequence: {seq}) {{ balance }} AccountCount {{ count }} }} }}"#),
        json!({}),
    )
    .await;
    assert_eq!(v["data"]["bank"]["Balance"]["balance"], 12, "{v}");
    assert_eq!(v["data"]["bank"]["AccountCount"]["count"], 1, "{v}");

    // Admin mints a REST command token over GraphQL.
    let v = gql(
        &app,
        Some("ops@idp"),
        r#"mutation { bank_mintToken(purpose: "command", type: "Deposit") }"#,
        json!({}),
    )
    .await;
    let bearer = v["data"]["bank_mintToken"]["bearer"]
        .as_str()
        .unwrap()
        .to_string();
    let (st, body) = call(
        &app,
        Request::post("/rest/v1/commands/Deposit")
            .header("authorization", format!("Bearer {bearer}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"payload": {"account_id": "a1", "amount": 3}}).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    // The token can't trigger another type.
    let (st, _) = call(
        &app,
        Request::post("/rest/v1/commands/Withdraw")
            .header("authorization", format!("Bearer {bearer}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"payload": {"account_id": "a1", "amount": 1}}).to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // REST read with a filter, cursor carries the epoch.
    let v = gql(
        &app,
        Some("ops@idp"),
        r#"mutation { bank_mintToken(purpose: "event_read", type: "MoneyDeposited") }"#,
        json!({}),
    )
    .await;
    let reader = v["data"]["bank_mintToken"]["bearer"]
        .as_str()
        .unwrap()
        .to_string();
    let (st, body) = call(
        &app,
        Request::get("/rest/v1/events/MoneyDeposited?filter=amount:greater_than:5")
            .header("authorization", format!("Bearer {reader}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["events"].as_array().unwrap().len(), 1);
    let cursor = body["cursor"].as_str().unwrap().to_string();
    let (st, body) = call(
        &app,
        Request::get(format!(
            "/rest/v1/events/MoneyDeposited?after={}",
            cursor.replace('@', "%40")
        ))
        .header("authorization", format!("Bearer {reader}"))
        .body(Body::empty())
        .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["events"].as_array().unwrap().len(), 1, "{body}");
    let (st, _) = call(
        &app,
        Request::get("/rest/v1/events/MoneyDeposited?after=bogus%400")
            .header("authorization", format!("Bearer {reader}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT, "stale epoch refused");

    // Superadmin directory.
    let v = gql(&app, Some("root@idp"), "{ boundedContexts }", json!({})).await;
    assert!(
        v["data"]["boundedContexts"].as_array().unwrap().len() >= 2,
        "{v}"
    );
    let v = gql(&app, Some("writer@idp"), "{ boundedContexts }", json!({})).await;
    assert_eq!(v["errors"][0]["extensions"]["code"], "FORBIDDEN", "{v}");
    // No credentials at all.
    let v = gql(&app, None, "{ bank { countEvents } }", json!({})).await;
    assert_eq!(v["errors"][0]["extensions"]["code"], "FORBIDDEN", "{v}");
}
