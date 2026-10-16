//! End-to-end proof of `skilj_temporal::poll_once` against two real
//! systems, neither mocked at the protocol level it matters for: a real,
//! ephemeral Temporal service (a `temporal-test-server` this file
//! downloads and launches itself, dev-only - see this crate's own
//! Cargo.toml) receives the actual `StartWorkflowExecution`/
//! `SignalWorkflowExecution` calls this crate makes, and proves they
//! really take effect there, not just that no error was returned. The
//! skilj *side* is a small local mock server, the same "wire protocol
//! only, no real skilj crate" treatment `skilj-tui`'s own
//! `tests/subscription.rs` already gives its GraphQL half - skilj's own
//! `GET /v1/events/consume`/`POST /v1/events/consume/ack` contract is
//! already exhaustively tested against a real server elsewhere
//! (`skilj/tests/event_fetch_rest.rs` and friends); this crate's own
//! job is proving it *calls* that contract correctly, not re-proving
//! skilj's own implementation of it.
//!
//! Downloads a small (~25MB) test-server binary on first run, cached
//! under `~/.cache/skilj-temporal-test-server` (never `/tmp` - see
//! CONTRIBUTING.md's own note on that directory's tmpfs-fill history)
//! for 15 days. Skips gracefully, the same register every other real-
//! external-service test in this workspace already uses (`DATABASE_URL`-
//! then-embedded-Postgres-then-skip), if that download can't reach the
//! network in a given environment.
//! `SKILJ_TEMPORAL_TEST_SERVER=/path/to/temporal-test-server` runs that
//! binary instead of downloading one; the Java SDK's GitHub releases
//! publish it too (`temporal-test-server_<version>_linux_amd64.tar.gz`).

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use skilj_temporal::{poll_once, EventTypeMapping, MappingAction};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::UntypedWorkflow;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for these tests")
    })
}

/// A launched `temporal-test-server`, stopped when this is dropped.
struct TemporalServer {
    /// gRPC target host:port of the server frontend.
    target: String,
    /// Held only for its `kill_on_drop` - dropping this struct stops the
    /// server, the same lifecycle `temporalio-sdk-core`'s own
    /// `EphemeralServer` used to give us.
    _child: tokio::process::Child,
}

/// Starts a real, ephemeral Temporal service - `None` (skip) if the
/// one-time binary download can't reach the network here, the same
/// tolerance this workspace's own embedded-Postgres tests already have
/// for their own external dependency.
///
/// This used to be `temporalio-sdk-core`'s own `ephemeral-server`
/// feature, but that dependency's `zip = "^8.4"` cannot be resolved any
/// more: crates.io yanked every `zip` release before 9.0.3 after three
/// HIGH archive-extraction advisories (Oct 2026), and no fixed 8.x
/// exists. So the provisioning is done here, mirroring what that feature
/// did - ask `temporal.download` which archive to fetch, cache the
/// extracted executable, launch it on a free port, wait for its gRPC
/// frontend - with the archive unpacked by `tar`/`flate2` (Unix) or the
/// fixed `zip` 9 (Windows) instead of the vulnerable 8.x line.
async fn start_temporal() -> Option<TemporalServer> {
    // `SKILJ_TEMPORAL_TEST_SERVER` names a `temporal-test-server` binary to
    // run instead of downloading one - for a network that blocks
    // temporal.download. It is also published with the Java SDK's GitHub
    // releases (`temporal-test-server_<version>_linux_amd64.tar.gz`).
    let exe = match std::env::var("SKILJ_TEMPORAL_TEST_SERVER") {
        Ok(path) => {
            let path = std::path::PathBuf::from(path);
            if !path.exists() {
                eprintln!(
                    "skipping: SKILJ_TEMPORAL_TEST_SERVER={} does not exist",
                    path.display()
                );
                return None;
            }
            path
        }
        Err(_) => match cached_test_server().await {
            Ok(path) => path,
            Err(e) => {
                eprintln!(
                    "skipping: downloading the ephemeral Temporal test server failed \
                     (no network egress to temporal.download, most likely): {e}"
                );
                return None;
            }
        },
    };

    let port = match free_port() {
        Ok(port) => port,
        Err(e) => {
            eprintln!("skipping: no free TCP port for the ephemeral Temporal test server: {e}");
            return None;
        }
    };
    let mut child = match tokio::process::Command::new(&exe)
        .arg(port.to_string())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            eprintln!("skipping: starting {} failed: {e}", exe.display());
            return None;
        }
    };

    let target = format!("127.0.0.1:{port}");
    let options = ConnectionOptions::new(
        format!("http://{target}")
            .parse::<temporalio_client::Url>()
            .unwrap(),
    )
    .build();
    // Try to connect every 100ms for 5s, the same readiness check
    // `EphemeralServer` used.
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if Connection::connect(options.clone()).await.is_ok() {
            return Some(TemporalServer {
                target,
                _child: child,
            });
        }
    }
    let _ = child.kill().await;
    eprintln!("skipping: the ephemeral Temporal test server at {target} did not become ready");
    None
}

/// Never `/tmp` - see this file's own doc comment.
fn cache_dir() -> std::path::PathBuf {
    let home = std::env::var("HOME").expect("HOME must be set");
    std::path::PathBuf::from(home).join(".cache/skilj-temporal-test-server")
}

/// The extracted test-server executable for this platform.
fn cache_path() -> std::path::PathBuf {
    let ext = if cfg!(windows) { ".exe" } else { "" };
    cache_dir().join(format!("temporal-test-server-sdk-rust-0.1.0{ext}"))
}

/// A TCP port the OS reports as free, the same `get_free_port` the SDK's
/// own ephemeral server used.
fn free_port() -> std::io::Result<u16> {
    Ok(std::net::TcpListener::bind(("0.0.0.0", 0))?
        .local_addr()?
        .port())
}

/// What `temporal.download` answers with: the archive to fetch and the
/// path of the executable inside it.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DownloadInfo {
    archive_url: String,
    file_to_extract: String,
}

/// Downloads and extracts the platform's test server, reusing the cached
/// copy (15 days, the SDK's own TTL) when it is still fresh. Guarded by a
/// `OnceCell` so the parallel test threads share one download instead of
/// racing each other into the same cache file.
async fn cached_test_server() -> Result<std::path::PathBuf, String> {
    static CELL: tokio::sync::OnceCell<Result<std::path::PathBuf, String>> =
        tokio::sync::OnceCell::const_new();
    CELL.get_or_init(download_test_server).await.clone()
}

/// The body of `cached_test_server`, run at most once per test process.
async fn download_test_server() -> Result<std::path::PathBuf, String> {
    let dest = cache_path();
    if let Ok(mtime) = std::fs::metadata(&dest).and_then(|m| m.modified()) {
        if mtime.elapsed().unwrap_or_default() < Duration::from_secs(60 * 60 * 24 * 15) {
            return Ok(dest);
        }
        let _ = std::fs::remove_file(&dest);
    }

    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "arm" | "aarch64" => "arm64",
        other => return Err(format!("unsupported architecture: {other}")),
    };
    let platform = match std::env::consts::OS {
        "windows" => "windows",
        "macos" => "darwin",
        _ => "linux",
    };
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| format!("could not build the download client: {e}"))?;
    let info: DownloadInfo = client
        .get(format!(
            "https://temporal.download/temporal-test-server/default\
             ?arch={arch}&platform={platform}&sdk-name=sdk-rust&sdk-version=0.1.0"
        ))
        .send()
        .await
        .map_err(|e| format!("could not reach temporal.download: {e}"))?
        .error_for_status()
        .map_err(|e| format!("temporal.download rejected the request: {e}"))?
        .json()
        .await
        .map_err(|e| format!("could not parse the temporal.download response: {e}"))?;

    let bytes = client
        .get(&info.archive_url)
        .send()
        .await
        .map_err(|e| format!("could not download {}: {e}", info.archive_url))?
        .error_for_status()
        .map_err(|e| format!("downloading {} failed: {e}", info.archive_url))?
        .bytes()
        .await
        .map_err(|e| format!("could not read {}: {e}", info.archive_url))?;

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("could not create {}: {e}", parent.display()))?;
    }
    extract_server(&info.archive_url, &info.file_to_extract, &bytes, &dest)
        .map_err(|e| format!("could not extract {}: {e}", info.file_to_extract))?;
    Ok(dest)
}

#[cfg(unix)]
fn extract_server(
    archive_url: &str,
    file_to_extract: &str,
    bytes: &[u8],
    dest: &std::path::Path,
) -> Result<(), String> {
    use std::os::unix::fs::OpenOptionsExt;

    if !archive_url.ends_with(".tar.gz") {
        return Err(format!("unsupported archive format: {archive_url}"));
    }
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(bytes));
    for entry in archive.entries().map_err(|e| e.to_string())? {
        let mut entry = entry.map_err(|e| e.to_string())?;
        if entry.path().map_err(|e| e.to_string())?.as_ref()
            == std::path::Path::new(file_to_extract)
        {
            // Mode 0o755, the same one the SDK's own downloader used - the
            // archive's own entry mode is not copied by `io::copy`.
            let mut out = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o755)
                .open(dest)
                .map_err(|e| e.to_string())?;
            std::io::copy(&mut entry, &mut out).map_err(|e| e.to_string())?;
            return Ok(());
        }
    }
    Err(format!("{file_to_extract} not found in {archive_url}"))
}

#[cfg(windows)]
fn extract_server(
    archive_url: &str,
    file_to_extract: &str,
    bytes: &[u8],
    dest: &std::path::Path,
) -> Result<(), String> {
    if !archive_url.ends_with(".zip") {
        return Err(format!("unsupported archive format: {archive_url}"));
    }
    let mut archive =
        zip::ZipArchive::new(std::io::Cursor::new(bytes)).map_err(|e| e.to_string())?;
    let mut entry = archive
        .by_name(file_to_extract)
        .map_err(|e| e.to_string())?;
    let mut out = std::fs::File::create(dest).map_err(|e| e.to_string())?;
    std::io::copy(&mut entry, &mut out).map_err(|e| e.to_string())?;
    Ok(())
}

async fn connect_temporal(target: &str) -> Client {
    let connection = Connection::connect(
        ConnectionOptions::new(target.parse::<temporalio_client::Url>().unwrap())
            .keep_alive(None)
            .build(),
    )
    .await
    .expect("connecting to the ephemeral Temporal test server must succeed");
    Client::new(connection, ClientOptions::new("default").build())
        .expect("building a Client from a fresh connection must succeed")
}

#[derive(Clone, Default)]
struct MockSkiljState {
    /// Keyed by the bearer credential each `EventTypeMapping` carries -
    /// simulates `EventReadToken` scoping (one token, one event type's
    /// own stream) without a real skilj server.
    queues: Arc<Mutex<HashMap<String, VecDeque<Value>>>>,
    /// Same keying - the `EventType::NAME` a real `EventReadToken` would
    /// be scoped to, echoed back as `ConsumeResponse::event_type_name`
    /// (`poll_once`'s own mismatch check against `EventTypeMapping::
    /// event_type`).
    event_types: Arc<Mutex<HashMap<String, String>>>,
    acked: Arc<Mutex<Vec<(String, i64)>>>,
}

async fn get_events_consume(
    State(state): State<MockSkiljState>,
    headers: HeaderMap,
) -> Json<Value> {
    let token = bearer_token(&headers);
    let events: Vec<Value> = state
        .queues
        .lock()
        .unwrap()
        .get_mut(&token)
        .map(|q| q.drain(..).collect())
        .unwrap_or_default();
    let event_type_name = state
        .event_types
        .lock()
        .unwrap()
        .get(&token)
        .cloned()
        .unwrap_or_default();
    Json(json!({ "events": events, "eventTypeName": event_type_name }))
}

async fn post_events_consume_ack(
    State(state): State<MockSkiljState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    let token = bearer_token(&headers);
    let sequence = body["sequence"].as_i64().unwrap();
    state.acked.lock().unwrap().push((token, sequence));
    StatusCode::OK
}

/// Registers `token` as scoped to `event_type` (mirroring a real
/// `EventReadToken`'s own fixed scope) and queues `events` for it in one
/// call, so every test site sets up both halves the mismatch check in
/// `poll_once` needs together.
fn enqueue(
    state: &MockSkiljState,
    token: &str,
    event_type: &str,
    events: impl Into<VecDeque<Value>>,
) {
    state
        .event_types
        .lock()
        .unwrap()
        .insert(token.to_string(), event_type.to_string());
    state
        .queues
        .lock()
        .unwrap()
        .insert(token.to_string(), events.into());
}

fn bearer_token(headers: &HeaderMap) -> String {
    headers
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap()
        .to_string()
}

async fn serve_mock_skilj(state: MockSkiljState) -> String {
    let app = Router::new()
        .route("/v1/events/consume", get(get_events_consume))
        .route("/v1/events/consume/ack", post(post_events_consume_ack))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// The scenario [docs/architecture.md §34](../../docs/architecture.md#skilj-temporal-plan) phase 2/3 exists for: an
/// `OrderPlaced` event starts a fresh Temporal workflow run, correlated
/// by the fixed `"{bounded_context}:{tag_key}:{tag_value}"` convention -
/// then a later `PaymentConfirmed` event, carrying the identical tag
/// value, signals that *same* running execution rather than starting a
/// second one or being misrouted, proving the correlation convention
/// actually threads two independently-mapped event types to one
/// workflow id.
#[test]
fn an_order_placed_event_starts_a_workflow_and_a_later_payment_confirmed_event_signals_it() {
    runtime().block_on(async {
        let Some(temporal_server) = start_temporal().await else {
            return;
        };
        let temporal = connect_temporal(&format!("http://{}", temporal_server.target)).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let start_token = "start-token".to_string();
        let signal_token = "signal-token".to_string();
        enqueue(
            &mock_state,
            &start_token,
            "OrderPlaced",
            [json!({
                "sequence": 1,
                "eventType": "OrderPlaced",
                "payload": { "orderId": "o-42" },
                "tags": [{ "key": "order", "value": "o-42" }],
            })],
        );
        enqueue(
            &mock_state,
            &signal_token,
            "PaymentConfirmed",
            [json!({
                "sequence": 2,
                "eventType": "PaymentConfirmed",
                "payload": { "orderId": "o-42", "amount": 30 },
                "tags": [{ "key": "order", "value": "o-42" }],
            })],
        );

        let start_mapping = EventTypeMapping {
            event_type: "OrderPlaced".to_string(),
            credential: start_token.clone(),
            correlation_tag_key: "order".to_string(),
            action: MappingAction::Start {
                workflow_type: "OrderFulfillment".to_string(),
                task_queue: "orders".to_string(),
            },
        };
        let signal_mapping = EventTypeMapping {
            event_type: "PaymentConfirmed".to_string(),
            credential: signal_token.clone(),
            correlation_tag_key: "order".to_string(),
            action: MappingAction::Signal {
                signal_name: "paymentConfirmed".to_string(),
            },
        };

        let http = reqwest::Client::new();

        let served = poll_once(&http, &skilj_base_url, &temporal, "orders", &start_mapping)
            .await
            .expect("starting the workflow must succeed");
        assert_eq!(served, 1);

        // Real proof, not just "no error": the workflow execution
        // actually exists in Temporal's own server, under the id the
        // correlation convention derives.
        let description = temporal
            .get_workflow_handle::<UntypedWorkflow>("orders:order:o-42".to_string())
            .describe(Default::default())
            .await
            .expect("the started workflow must be describable");
        assert_eq!(description.id(), "orders:order:o-42");

        let served = poll_once(&http, &skilj_base_url, &temporal, "orders", &signal_mapping)
            .await
            .expect("signaling the already-running workflow must succeed");
        assert_eq!(served, 1);

        let acked = mock_state.acked.lock().unwrap().clone();
        assert_eq!(
            acked,
            vec![(start_token, 1), (signal_token, 2),],
            "both events must have been acknowledged, by their own token, after dispatch"
        );

        let _ = temporal_server; // keep alive until here; dropped (and the process killed) at scope end
    });
}

/// The negative case backing the correlation convention's own
/// `bounded_context` component: an event whose tag value matches but
/// whose bounded context doesn't derives a *different* workflow id, so
/// it starts an independent execution rather than colliding with the
/// first test's `"o-42"`.
#[test]
fn a_different_bounded_context_with_the_same_tag_value_starts_a_different_workflow() {
    runtime().block_on(async {
        let Some(temporal_server) = start_temporal().await else {
            return;
        };
        let temporal = connect_temporal(&format!("http://{}", temporal_server.target)).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;
        let token = "token".to_string();
        enqueue(
            &mock_state,
            &token,
            "OrderPlaced",
            [json!({
                "sequence": 1,
                "eventType": "OrderPlaced",
                "payload": { "orderId": "o-1" },
                "tags": [{ "key": "order", "value": "o-1" }],
            })],
        );
        let mapping = EventTypeMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token,
            correlation_tag_key: "order".to_string(),
            action: MappingAction::Start {
                workflow_type: "OrderFulfillment".to_string(),
                task_queue: "orders".to_string(),
            },
        };
        let http = reqwest::Client::new();
        poll_once(&http, &skilj_base_url, &temporal, "returns", &mapping)
            .await
            .unwrap();

        let description = temporal
            .get_workflow_handle::<UntypedWorkflow>("returns:order:o-1".to_string())
            .describe(Default::default())
            .await
            .expect("the workflow must exist under the bounded-context-qualified id");
        assert_eq!(description.id(), "returns:order:o-1");

        let _ = temporal_server;
    });
}

/// `BridgeError::NoCorrelationTag`'s own design point (this crate's own
/// `poll_once` doc comment): an event whose declared `correlation_tag_key`
/// isn't present among its own tags is still acknowledged - not left to
/// redeliver forever chasing a value the event itself never carried -
/// and, just as importantly, never reaches Temporal at all for that
/// event (no workflow is started under some nonsensical id).
#[test]
fn an_event_with_no_matching_correlation_tag_is_acknowledged_but_never_dispatched() {
    runtime().block_on(async {
        let Some(temporal_server) = start_temporal().await else {
            return;
        };
        let temporal = connect_temporal(&format!("http://{}", temporal_server.target)).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;
        let token = "token".to_string();
        enqueue(
            &mock_state,
            &token,
            "OrderPlaced",
            [json!({
                "sequence": 7,
                "eventType": "OrderPlaced",
                "payload": { "orderId": "o-9" },
                // No "order" tag at all - the declared correlation_tag_key
                // below can't find anything to derive a workflow id from.
                "tags": [{ "key": "company", "value": "acme" }],
            })],
        );
        let mapping = EventTypeMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token.clone(),
            correlation_tag_key: "order".to_string(),
            action: MappingAction::Start {
                workflow_type: "OrderFulfillment".to_string(),
                task_queue: "orders".to_string(),
            },
        };
        let http = reqwest::Client::new();
        let served = poll_once(&http, &skilj_base_url, &temporal, "orders", &mapping)
            .await
            .expect("an unmapped correlation tag must not surface as an error");
        assert_eq!(served, 1);

        assert_eq!(
            mock_state.acked.lock().unwrap().clone(),
            vec![(token, 7)],
            "the event must still be acknowledged even though it was never dispatched"
        );

        // No workflow was ever started - describing any id this event
        // could plausibly have derived finds nothing.
        let result = temporal
            .get_workflow_handle::<UntypedWorkflow>("orders:order:o-9".to_string())
            .describe(Default::default())
            .await;
        assert!(
            result.is_err(),
            "no workflow should exist - the event was never dispatched"
        );

        let _ = temporal_server;
    });
}

/// A code-review finding this crate's own history is worth pinning down
/// as a regression test: a `Start` redelivered while the original
/// execution is *still running* must not error - `id_conflict_policy`
/// (`WorkflowIdConflictPolicy::UseExisting`) is what makes that so. (The
/// sibling case - redelivered *after* the original already closed,
/// where `id_reuse_policy`'s `RejectDuplicate` plus `dispatch`'s own
/// `AlreadyStarted` handling is what keeps that a no-op too - would need
/// a real Worker actually completing the workflow to exercise, which
/// would pull in the worker/Activity-authoring SDK this crate
/// deliberately depends on for tests only, not production; verified by
/// reading Temporal's own enum documentation instead. See this crate's
/// own `dispatch` doc comment.)
#[test]
fn a_redelivered_start_while_the_workflow_is_still_running_does_not_error() {
    runtime().block_on(async {
        let Some(temporal_server) = start_temporal().await else {
            return;
        };
        let temporal = connect_temporal(&format!("http://{}", temporal_server.target)).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;
        let token = "token".to_string();
        let event = json!({
            "sequence": 1,
            "eventType": "OrderPlaced",
            "payload": { "orderId": "o-redelivered" },
            "tags": [{ "key": "order", "value": "o-redelivered" }],
        });
        let mapping = EventTypeMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token.clone(),
            correlation_tag_key: "order".to_string(),
            action: MappingAction::Start {
                workflow_type: "OrderFulfillment".to_string(),
                task_queue: "orders".to_string(),
            },
        };
        let http = reqwest::Client::new();

        enqueue(&mock_state, &token, "OrderPlaced", [event.clone()]);
        poll_once(&http, &skilj_base_url, &temporal, "orders", &mapping)
            .await
            .expect("the first Start must succeed");

        // Redeliver the identical event - same sequence, same tags, same
        // derived workflow id. Nothing is running a Worker against this
        // workflow's own task queue, so the first execution is still
        // open when this second Start attempt lands.
        enqueue(&mock_state, &token, "OrderPlaced", [event]);
        poll_once(&http, &skilj_base_url, &temporal, "orders", &mapping)
            .await
            .expect("a Start redelivered while the workflow is still running must not error");

        let _ = temporal_server;
    });
}

/// `poll_once`'s own mismatch check: an `EventTypeMapping` whose
/// `event_type` doesn't match what its `credential` is actually scoped
/// to - a real `EventReadToken` mixup - is rejected outright rather than
/// silently misrouting whatever events the credential really serves.
#[test]
fn a_mapping_whose_event_type_does_not_match_its_credential_is_rejected() {
    runtime().block_on(async {
        let Some(temporal_server) = start_temporal().await else {
            return;
        };
        let temporal = connect_temporal(&format!("http://{}", temporal_server.target)).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;
        let token = "token".to_string();
        // The token is actually scoped to PaymentConfirmed...
        enqueue(&mock_state, &token, "PaymentConfirmed", []);
        // ...but this mapping declares OrderPlaced.
        let mapping = EventTypeMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token,
            correlation_tag_key: "order".to_string(),
            action: MappingAction::Start {
                workflow_type: "OrderFulfillment".to_string(),
                task_queue: "orders".to_string(),
            },
        };
        let http = reqwest::Client::new();
        let error = poll_once(&http, &skilj_base_url, &temporal, "orders", &mapping)
            .await
            .expect_err("a declared/actual event_type mismatch must be rejected");
        assert!(error.to_string().contains("OrderPlaced"));
        assert!(error.to_string().contains("PaymentConfirmed"));

        let _ = temporal_server;
    });
}

/// docs/architecture.md §99: a manual-ack consume claims what it serves,
/// so an event whose dispatch failed isn't served again until the claim
/// lapses (five minutes by default) - this mock never serves it again at
/// all. `run` keeps it and retries it next cycle. Here the `Signal`
/// mapping is polled before the `Start` one, so the signal's first attempt
/// fails (no workflow yet) and must still be delivered once it exists.
#[test]
fn run_retries_a_failed_dispatch_without_consuming_it_again() {
    runtime().block_on(async {
        let Some(temporal_server) = start_temporal().await else {
            return;
        };
        let temporal = connect_temporal(&format!("http://{}", temporal_server.target)).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;
        enqueue(
            &mock_state,
            "start-token",
            "OrderPlaced",
            [json!({
                "sequence": 1,
                "eventType": "OrderPlaced",
                "payload": { "orderId": "o-race" },
                "tags": [{ "key": "order", "value": "o-race" }],
            })],
        );
        enqueue(
            &mock_state,
            "signal-token",
            "PaymentConfirmed",
            [json!({
                "sequence": 2,
                "eventType": "PaymentConfirmed",
                "payload": { "orderId": "o-race", "amount": 30 },
                "tags": [{ "key": "order", "value": "o-race" }],
            })],
        );
        let mappings = vec![
            EventTypeMapping {
                event_type: "PaymentConfirmed".to_string(),
                credential: "signal-token".to_string(),
                correlation_tag_key: "order".to_string(),
                action: MappingAction::Signal {
                    signal_name: "paymentConfirmed".to_string(),
                },
            },
            EventTypeMapping {
                event_type: "OrderPlaced".to_string(),
                credential: "start-token".to_string(),
                correlation_tag_key: "order".to_string(),
                action: MappingAction::Start {
                    workflow_type: "OrderFulfillment".to_string(),
                    task_queue: "orders".to_string(),
                },
            },
        ];
        let target = temporal_server.target.clone();
        tokio::spawn(async move {
            let temporal = connect_temporal(&format!("http://{target}")).await;
            skilj_temporal::run(
                &skilj_base_url,
                &temporal,
                "rescue",
                &mappings,
                Duration::from_millis(100),
            )
            .await;
        });

        let mut acked = Vec::new();
        for _ in 0..150 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            acked = mock_state.acked.lock().unwrap().clone();
            if acked.contains(&("signal-token".to_string(), 2)) {
                break;
            }
        }
        assert!(
            acked.contains(&("signal-token".to_string(), 2)),
            "the signal whose first dispatch failed was never retried: {acked:?}"
        );
        let _ = temporal;
        let _ = temporal_server;
    });
}

/// `run_until` returns once asked (docs/architecture.md §129): it starts
/// the workflow for the event it was served and acks it, then - idle, a
/// 60 s poll interval ahead - stops at once.
#[test]
fn run_until_stops_when_asked() {
    runtime().block_on(async {
        let Some(temporal_server) = start_temporal().await else {
            return;
        };
        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;
        enqueue(
            &mock_state,
            "start-token",
            "OrderPlaced",
            [json!({
                "sequence": 1,
                "eventType": "OrderPlaced",
                "payload": { "orderId": "o-stop" },
                "tags": [{ "key": "order", "value": "o-stop" }],
            })],
        );
        let mappings = vec![EventTypeMapping {
            event_type: "OrderPlaced".to_string(),
            credential: "start-token".to_string(),
            correlation_tag_key: "order".to_string(),
            action: MappingAction::Start {
                workflow_type: "OrderFulfillment".to_string(),
                task_queue: "orders".to_string(),
            },
        }];
        let target = temporal_server.target.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn(async move {
            let temporal = connect_temporal(&format!("http://{target}")).await;
            skilj_temporal::run_until(
                &skilj_base_url,
                &temporal,
                "stopping",
                &mappings,
                Duration::from_secs(60),
                async {
                    let _ = stopped.await;
                },
            )
            .await;
        });
        for _ in 0..150 {
            if !mock_state.acked.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            mock_state.acked.lock().unwrap().as_slice(),
            &[("start-token".to_string(), 1)]
        );
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run_until must return once asked")
            .unwrap();
        let _ = temporal_server;
    });
}

/// docs/architecture.md §147: a `Signal` for a workflow that doesn't
/// exist (never started, or already completed) fails on every attempt.
/// With a bounded `retry_policy` it's skipped - acknowledged - once the
/// policy is exhausted, and the event behind it gets its turn instead of
/// waiting forever.
#[test]
fn run_with_a_bounded_policy_skips_an_event_that_keeps_failing() {
    runtime().block_on(async {
        let Some(temporal_server) = start_temporal().await else {
            return;
        };
        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;
        enqueue(
            &mock_state,
            "signal-token",
            "PaymentConfirmed",
            [1, 2].map(|sequence| {
                json!({
                    "sequence": sequence,
                    "eventType": "PaymentConfirmed",
                    "payload": { "orderId": format!("o-gone-{sequence}") },
                    "tags": [{ "key": "order", "value": format!("o-gone-{sequence}") }],
                })
            }),
        );
        let mappings = vec![EventTypeMapping {
            event_type: "PaymentConfirmed".to_string(),
            credential: "signal-token".to_string(),
            correlation_tag_key: "order".to_string(),
            action: MappingAction::Signal {
                signal_name: "paymentConfirmed".to_string(),
            },
        }];
        let target = temporal_server.target.clone();
        tokio::spawn(async move {
            let temporal = connect_temporal(&format!("http://{target}")).await;
            skilj_temporal::run_with_retry(
                &skilj_base_url,
                &temporal,
                "skipping",
                &mappings,
                Duration::from_millis(50),
                &skilj_retry::RetryPolicy::bounded(
                    Duration::from_millis(10),
                    1.0,
                    Duration::from_millis(10),
                    2,
                ),
            )
            .await;
        });

        let both = [
            ("signal-token".to_string(), 1),
            ("signal-token".to_string(), 2),
        ];
        let mut acked = Vec::new();
        for _ in 0..150 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            acked = mock_state.acked.lock().unwrap().clone();
            if both.iter().all(|a| acked.contains(a)) {
                break;
            }
        }
        assert!(
            both.iter().all(|a| acked.contains(a)),
            "both undeliverable signals must have been skipped: {acked:?}"
        );
        let _ = temporal_server;
    });
}
