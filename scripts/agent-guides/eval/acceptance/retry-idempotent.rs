//! Acceptance tests for tasks/retry-idempotent.md, copied into the
//! project's `tests/` by check.sh.
//!
//! The server is a mock on a local socket that answers each request as
//! the test scripts it: a lost response (the request arrives, the
//! connection is closed without an answer), a status, or a body.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone, Copy)]
enum Answer {
    /// The request is read and recorded; the connection closes unanswered.
    Lost,
    Status(u16),
}

#[derive(Debug, Clone)]
struct Request {
    line: String,
    headers: Vec<(String, String)>,
    body: serde_json::Value,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

const ACCEPTED: &str = r#"{"accepted":true,"triggeredEventSequences":[1],"deduplicated":false}"#;

/// Starts the mock; answers `script` in order, then 200 with ACCEPTED.
async fn mock(script: Vec<Answer>) -> (String, Arc<Mutex<Vec<Request>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = seen.clone();
    tokio::spawn(async move {
        let mut script = script.into_iter();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let Some(request) = read_request(&mut socket).await else { continue };
            recorded.lock().unwrap().push(request);
            let (status, body) = match script.next() {
                Some(Answer::Lost) => {
                    drop(socket);
                    continue;
                }
                Some(Answer::Status(status)) => (status, r#"{"error":"scripted"}"#),
                None => (200, ACCEPTED),
            };
            let response = format!(
                "HTTP/1.1 {status} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        }
    });
    (base_url, seen)
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<Request> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = socket.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let line = lines.next()?.to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let length: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    while buffer.len() < header_end + length {
        let n = socket.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..n]);
    }
    let body = serde_json::from_slice(&buffer[header_end..]).unwrap_or(serde_json::Value::Null);
    Some(Request { line, headers, body })
}

fn requests(seen: &Arc<Mutex<Vec<Request>>>) -> Vec<Request> {
    seen.lock().unwrap().clone()
}

/// A single Deposit trigger, with the token, and an Idempotency-Key.
fn assert_is_the_deposit(request: &Request) -> String {
    assert!(
        request.line.starts_with("POST /v1/commands/trigger "),
        "{}",
        request.line
    );
    assert_eq!(request.header("authorization"), Some("Bearer tok"), "{request:?}");
    assert_eq!(
        request.body["payload"],
        serde_json::json!({ "wallet_id": "w1", "amount": 100 }),
        "{request:?}"
    );
    let key = request.header("idempotency-key").unwrap_or_default();
    assert!(!key.is_empty(), "no Idempotency-Key: {request:?}");
    key.to_string()
}

async fn deposit(base_url: &str, deposit_id: &str) -> Option<String> {
    template_check::client::deposit(base_url, "tok", "w1", 100, deposit_id)
        .await
        .ok()
}

#[tokio::test]
async fn a_lost_response_is_retried_with_the_same_key() {
    let (base_url, seen) = mock(vec![Answer::Lost]).await;
    let answer = deposit(&base_url, "dep-1").await.expect("the retry is answered");
    assert!(answer.contains(r#""accepted":true"#), "{answer}");
    let requests = requests(&seen);
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(assert_is_the_deposit(&requests[0]), assert_is_the_deposit(&requests[1]));
}

#[tokio::test]
async fn a_5xx_is_retried_with_the_same_key() {
    let (base_url, seen) = mock(vec![Answer::Status(503), Answer::Status(500)]).await;
    let answer = deposit(&base_url, "dep-1").await.expect("the retry is answered");
    assert!(answer.contains(r#""accepted":true"#), "{answer}");
    let requests = requests(&seen);
    assert_eq!(requests.len(), 3, "{requests:?}");
    let key = assert_is_the_deposit(&requests[0]);
    assert_eq!(assert_is_the_deposit(&requests[1]), key);
    assert_eq!(assert_is_the_deposit(&requests[2]), key);
}

/// Running the binary again with a deposit's id is the same deposit.
#[tokio::test]
async fn the_key_is_the_deposits() {
    let (base_url, seen) = mock(vec![]).await;
    deposit(&base_url, "dep-1").await.expect("answered");
    deposit(&base_url, "dep-1").await.expect("answered");
    deposit(&base_url, "dep-2").await.expect("answered");
    let keys: Vec<String> = requests(&seen).iter().map(assert_is_the_deposit).collect();
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], keys[1], "the same deposit_id, the same key");
    assert_ne!(keys[0], keys[2], "another deposit_id, another key");
}

#[tokio::test]
async fn a_4xx_is_not_retried() {
    let (base_url, seen) = mock(vec![Answer::Status(400), Answer::Status(400)]).await;
    assert!(deposit(&base_url, "dep-1").await.is_none(), "a refusal is an error");
    assert_eq!(requests(&seen).len(), 1);
}
