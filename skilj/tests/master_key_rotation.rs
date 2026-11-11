//! Rotating the encryption master key (docs/architecture.md §230): the
//! master key lives in a key provider, every data key records the
//! provider and key version it is wrapped under, and the background task
//! rewraps every data key after a rotation and then retires the old
//! versions at the provider - after a grace period, or at once. Every step
//! is an event in the admin bounded context.
//!
//! The Vault provider runs against a small in-process imitation of Vault's
//! Transit API (`FakeTransit`), which keeps the behaviour that matters here:
//! versioned ciphertexts, batch rewrap, `min_decryption_version` refusing
//! older versions, and token checks.
//!
//! Each test gets a database of its own: the master key state is one row
//! for the whole database.

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::SubsecRound;
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use serde_json::{json, Value};
use skilj::{
    EncryptionMasterKey, IdpConfig, InMemoryKeyProvider, KeyFileProvider, KeyProvider, KeyService,
    MasterKeyRotation, SigningAlgorithm, Skilj, SkiljBuilder, VaultTransitProvider,
};
use skilj_core::bootstrap::{ContextCreator, ADMIN_BOUNDED_CONTEXT_NAME};
use skilj_core::db::{self, Pool};
use skilj_core::encryption::{self, WrappedDataKey};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::generate_token_id;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tower::ServiceExt;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for master_key_rotation tests")
    })
}

/// A database of the test's own, or `None` (with a stderr note).
async fn test_db(name: &str) -> Option<(String, Pool)> {
    let url = skilj_test_support::database_url(name).await?;
    let pool = match db::connect(&url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to the test database failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating the test database failed: {e}");
        return None;
    }
    Some((url, pool))
}

fn test_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now().trunc_subsecs(6)
}

fn temp_key_file() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("skilj-master-keys-{}.json", generate_token_id()))
}

async fn insert_bounded_context(pool: &Pool) -> String {
    let bc = BoundedContext {
        name: format!("customers_{}", generate_token_id()),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    bc.name
}

/// A value sealed under `subject`'s data key, provisioned the way a
/// sensitive payload's write does.
async fn seal_for(pool: &Pool, bc: &str, subject: &str, keys: &KeyService) -> String {
    let (_, _, data_key) = db::get_or_create_encryption_key(pool, bc, "user", subject, keys)
        .await
        .unwrap();
    encryption::encrypt_leaf(&data_key, &format!("{subject}@example.com"))
}

/// `ciphertext` opened with `subject`'s data key, as the database holds it now.
async fn open_for(
    pool: &Pool,
    bc: &str,
    subject: &str,
    keys: &KeyService,
    ciphertext: &str,
) -> String {
    let (_, _, data_key) = db::get_or_create_encryption_key(pool, bc, "user", subject, keys)
        .await
        .unwrap();
    encryption::decrypt_leaf(&data_key, ciphertext).unwrap()
}

/// `(subject, provider, version, wrapped)` of every active data key.
async fn wraps(pool: &Pool, bc: &str) -> Vec<(String, Option<String>, Option<i32>, Vec<u8>)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT subject_value, key_provider, master_key_version, wrapped_key \
         FROM \"bc_{bc}\".encryption_keys WHERE status = 'active' ORDER BY subject_value"
    )))
    .fetch_all(pool)
    .await
    .unwrap()
}

/// The payloads of the admin context's events of type `name`, oldest first.
async fn admin_events(pool: &Pool, name: &str) -> Vec<Value> {
    db::list_events_for_bounded_context(pool, ADMIN_BOUNDED_CONTEXT_NAME)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.event_type.name == name)
        .map(|e| serde_json::from_str(&e.payload).unwrap())
        .collect()
}

/// Waits until the master key state satisfies `done`.
async fn wait_for_state(
    pool: &Pool,
    provider: &str,
    what: &str,
    done: impl Fn(&db::MasterKeyState) -> bool,
) {
    for _ in 0..600 {
        if let Some(state) = db::master_key_state(pool, provider).await.unwrap() {
            if done(&state) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "timed out waiting for {what}: {:?}\nevery key: {:#?}",
        db::master_key_state(pool, provider).await.unwrap(),
        db::master_key_states(pool).await.unwrap()
    );
}

fn quick(grace: Duration) -> MasterKeyRotation {
    MasterKeyRotation::new()
        .check_interval(Duration::from_millis(50))
        .retire_after(grace)
}

/// A builder with a small pool: this file's tests start many instances at
/// once against one Postgres, as the other suites' do.
fn builder(url: &str, keys: KeyService, rotation: MasterKeyRotation) -> SkiljBuilder {
    Skilj::builder(url.to_string())
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
        .key_service(keys)
        .master_key_rotation(rotation)
}

async fn refused(builder: SkiljBuilder) -> String {
    match builder.build().await {
        Ok(_) => panic!("startup was expected to be refused"),
        Err(err) => {
            if err.code() == "database_error" {
                eprintln!("database error: {err}");
            }
            err.code().to_string()
        }
    }
}

// --- an imitation of Vault's Transit API ---

#[derive(Default)]
struct Transit {
    /// Stands in for this Vault's key material: another fake's differs.
    material: String,
    /// The keys that exist besides `skilj`.
    created: Vec<String>,
    /// One entry per version; the "ciphertext" is the plaintext in hex,
    /// since only the version handling is under test here.
    latest: u32,
    min_decryption: u32,
    calls: Vec<String>,
    /// Every `encrypt` and `decrypt` request: the operation, the key and
    /// how many items it carried - one, or a batch's.
    requests: Vec<(&'static str, String, usize)>,
    /// How long every `encrypt` and `decrypt` request takes to answer.
    delay: Duration,
    /// A request to fail, once: the operation, how many of its requests
    /// to answer first, and the status to fail with.
    fault: Option<(&'static str, usize, u16)>,
}

impl Transit {
    /// Whether this request to `op` is the one to fail, and with what.
    fn faulted(&mut self, op: &'static str) -> Option<(StatusCode, Json<Value>)> {
        let (faulted, left, status) = self.fault.as_mut()?;
        if *faulted != op {
            return None;
        }
        if *left > 0 {
            *left -= 1;
            return None;
        }
        let status = StatusCode::from_u16(*status).unwrap();
        self.fault = None;
        Some((status, Json(json!({ "errors": ["injected"] }))))
    }
}

/// A batch's answer, as Vault gives it: 400 when an item failed - unless
/// `partial_failure_response_code` says otherwise and some item succeeded.
fn batch_answer(body: &Value, results: Vec<Value>) -> (StatusCode, Json<Value>) {
    let failed = results.iter().filter(|r| r.get("error").is_some()).count();
    let status = match (failed, body["partial_failure_response_code"].as_u64()) {
        (0, _) => StatusCode::OK,
        (f, Some(code)) if f < results.len() => StatusCode::from_u16(code as u16).unwrap(),
        _ => StatusCode::BAD_REQUEST,
    };
    (
        status,
        Json(json!({ "data": { "batch_results": results } })),
    )
}

type Fake = Arc<Mutex<Transit>>;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

fn denied(headers: &HeaderMap) -> Option<(StatusCode, Json<Value>)> {
    (headers.get("x-vault-token").and_then(|t| t.to_str().ok()) != Some("test-token")).then(|| {
        (
            StatusCode::FORBIDDEN,
            Json(json!({ "errors": ["permission denied"] })),
        )
    })
}

fn decrypt(transit: &Transit, ciphertext: &str) -> Result<Vec<u8>, String> {
    let rest = ciphertext
        .strip_prefix("vault:v")
        .ok_or("not a ciphertext")?;
    let (version, payload) = rest.split_once(':').ok_or("not a ciphertext")?;
    let version: u32 = version.parse().map_err(|_| "bad version")?;
    if version < transit.min_decryption {
        return Err("ciphertext or signature version is disallowed by policy (too old)".into());
    }
    // Another Vault's key under the same path: its material doesn't match.
    let (material, payload) = payload.split_once('.').ok_or("not a ciphertext")?;
    if material != transit.material {
        return Err("cipher: message authentication failed".into());
    }
    Ok(unhex(payload))
}

fn encrypt(transit: &Transit, plaintext: &[u8]) -> String {
    format!(
        "vault:v{}:{}.{}",
        transit.latest,
        transit.material,
        hex(plaintext)
    )
}

fn b64(bytes: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(TABLE[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn unb64(text: &str) -> Vec<u8> {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let digits: Vec<u32> = text
        .bytes()
        .filter(|b| *b != b'=')
        .map(|b| TABLE.iter().position(|t| *t == b).unwrap() as u32)
        .collect();
    for chunk in digits.chunks(4) {
        let mut n = 0u32;
        for (i, d) in chunk.iter().enumerate() {
            n |= d << (18 - 6 * i);
        }
        for i in 0..chunk.len().saturating_sub(1) {
            out.push((n >> (16 - 8 * i)) as u8);
        }
    }
    out
}

async fn serve_transit() -> (String, Fake) {
    let fake: Fake = Arc::new(Mutex::new(Transit {
        material: generate_token_id(),
        created: Vec::new(),
        latest: 1,
        min_decryption: 1,
        calls: Vec::new(),
        requests: Vec::new(),
        delay: Duration::ZERO,
        fault: None,
    }));
    let app = Router::new()
        .route(
            "/v1/transit/encrypt/{key}",
            post(|State(f): State<Fake>, headers: HeaderMap, Path(key): Path<String>, Json(body): Json<Value>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let delay = f.lock().unwrap().delay;
                tokio::time::sleep(delay).await;
                let mut t = f.lock().unwrap();
                t.calls.push("encrypt".into());
                if let Some(fault) = t.faulted("encrypt") {
                    return fault;
                }
                if let Some(batch) = body.get("batch_input").and_then(Value::as_array) {
                    t.requests.push(("encrypt", key, batch.len()));
                    let results = batch
                        .iter()
                        .map(|item| {
                            let plaintext = unb64(item["plaintext"].as_str().unwrap());
                            json!({ "ciphertext": encrypt(&t, &plaintext), "key_version": t.latest })
                        })
                        .collect();
                    return batch_answer(&body, results);
                }
                t.requests.push(("encrypt", key, 1));
                let plaintext = unb64(body["plaintext"].as_str().unwrap());
                let ciphertext = encrypt(&t, &plaintext);
                (StatusCode::OK, Json(json!({ "data": { "ciphertext": ciphertext, "key_version": t.latest } })))
            }),
        )
        .route(
            "/v1/transit/decrypt/{key}",
            post(|State(f): State<Fake>, headers: HeaderMap, Path(key): Path<String>, Json(body): Json<Value>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let delay = f.lock().unwrap().delay;
                tokio::time::sleep(delay).await;
                let mut t = f.lock().unwrap();
                t.calls.push("decrypt".into());
                if let Some(fault) = t.faulted("decrypt") {
                    return fault;
                }
                if let Some(batch) = body.get("batch_input").and_then(Value::as_array) {
                    t.requests.push(("decrypt", key, batch.len()));
                    let results = batch
                        .iter()
                        .map(|item| match decrypt(&t, item["ciphertext"].as_str().unwrap()) {
                            Ok(plaintext) => json!({ "plaintext": b64(&plaintext) }),
                            Err(e) => json!({ "error": e }),
                        })
                        .collect();
                    return batch_answer(&body, results);
                }
                t.requests.push(("decrypt", key, 1));
                match decrypt(&t, body["ciphertext"].as_str().unwrap()) {
                    Ok(plaintext) => (StatusCode::OK, Json(json!({ "data": { "plaintext": b64(&plaintext) } }))),
                    Err(e) => (StatusCode::BAD_REQUEST, Json(json!({ "errors": [e] }))),
                }
            }),
        )
        .route(
            "/v1/transit/rewrap/{key}",
            post(|State(f): State<Fake>, headers: HeaderMap, Json(body): Json<Value>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let mut t = f.lock().unwrap();
                t.calls.push("rewrap".into());
                let inputs: Vec<Value> = match body.get("batch_input") {
                    Some(batch) => batch.as_array().unwrap().clone(),
                    None => vec![json!({ "ciphertext": body["ciphertext"] })],
                };
                let results: Vec<Value> = inputs
                    .iter()
                    .map(|item| match decrypt(&t, item["ciphertext"].as_str().unwrap()) {
                        Ok(plaintext) => json!({ "ciphertext": encrypt(&t, &plaintext), "key_version": t.latest }),
                        Err(e) => json!({ "error": e }),
                    })
                    .collect();
                // As Vault: a single (non-batch) request that fails answers
                // 400 with the error; a batch as `batch_answer` has it.
                if body.get("batch_input").is_none() {
                    return match results[0].get("error") {
                        Some(e) => (StatusCode::BAD_REQUEST, Json(json!({ "errors": [e] }))),
                        None => (StatusCode::OK, Json(json!({ "data": results[0] }))),
                    };
                }
                batch_answer(&body, results)
            }),
        )
        .route(
            "/v1/transit/keys/{key}",
            get(|State(f): State<Fake>, headers: HeaderMap, Path(key): Path<String>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let t = f.lock().unwrap();
                if key != "skilj" && !t.created.contains(&key) {
                    return (StatusCode::NOT_FOUND, Json(json!({ "errors": [] })));
                }
                (
                    StatusCode::OK,
                    Json(json!({ "data": { "latest_version": t.latest, "min_decryption_version": t.min_decryption } })),
                )
            })
            .post(|State(f): State<Fake>, headers: HeaderMap, Path(key): Path<String>, Json(body): Json<Value>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let mut t = f.lock().unwrap();
                t.calls.push(format!("create {key} {}", body["type"].as_str().unwrap_or("")));
                t.created.push(key);
                (StatusCode::OK, Json(json!({ "data": {} })))
            })
            .delete(|State(f): State<Fake>, headers: HeaderMap, Path(key): Path<String>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let mut t = f.lock().unwrap();
                if !t.calls.iter().any(|c| c == &format!("allow deletion {key}")) {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({ "errors": ["deletion is not allowed for this key"] })),
                    );
                }
                t.calls.push(format!("delete {key}"));
                (StatusCode::NO_CONTENT, Json(json!(null)))
            }),
        )
        .route(
            "/v1/transit/keys/{key}/rotate",
            post(|State(f): State<Fake>, headers: HeaderMap| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let mut t = f.lock().unwrap();
                t.calls.push("rotate".into());
                t.latest += 1;
                (StatusCode::OK, Json(json!({ "data": { "latest_version": t.latest } })))
            }),
        )
        .route(
            "/v1/transit/keys/{key}/config",
            post(|State(f): State<Fake>, headers: HeaderMap, Path(key): Path<String>, Json(body): Json<Value>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let mut t = f.lock().unwrap();
                t.calls.push("config".into());
                if body["deletion_allowed"] == true {
                    t.calls.push(format!("allow deletion {key}"));
                }
                if let Some(min) = body["min_decryption_version"].as_u64() {
                    t.min_decryption = min as u32;
                }
                (StatusCode::OK, Json(json!({ "data": {} })))
            }),
        )
        .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (address, fake)
}

// --- the providers on their own ---

#[test]
fn the_vault_provider_wraps_rewraps_in_batches_and_is_refused_a_retired_version() {
    runtime().block_on(async {
        let (address, fake) = serve_transit().await;
        let vault = VaultTransitProvider::new(&address, "skilj", "test-token");
        assert_eq!(vault.name(), "vault-transit:transit/skilj");

        let data_key = [42u8; 32];
        let old = vault.wrap(&data_key).await.unwrap();
        assert_eq!(old.version, 1);
        assert_eq!(vault.unwrap(&old).await.unwrap(), data_key);

        assert_eq!(vault.rotate().await.unwrap(), 2);
        let rewrapped = vault.rewrap(&[old.clone(), old.clone()]).await.unwrap();
        assert_eq!(rewrapped.len(), 2);
        let new = rewrapped.into_iter().next().unwrap().unwrap();
        assert_eq!(new.version, 2);

        vault.retire_below(2).await.unwrap();
        assert!(matches!(
            vault.unwrap(&old).await,
            Err(encryption::Error::KeyVersionRetired { version: 1 })
        ));
        assert_eq!(vault.unwrap(&new).await.unwrap(), data_key);
        let versions = vault.versions().await.unwrap();
        assert_eq!((versions.latest, versions.min_decryption), (2, 2));
        assert!(fake.lock().unwrap().calls.iter().any(|c| c == "rewrap"));

        // Another key on the same mount: created, then deleted - which
        // Vault only allows once deletion is allowed on the key.
        let tenant = vault.key("tenant-a").unwrap();
        assert_eq!(tenant.name(), "vault-transit:transit/tenant-a");
        assert!(tenant.create().await.unwrap(), "created");
        assert!(
            !tenant.create().await.unwrap(),
            "already there: not created again"
        );
        assert!(
            !vault.create().await.unwrap(),
            "an existing key is not claimed"
        );
        tenant.delete().await.unwrap();
        let calls = fake.lock().unwrap().calls.clone();
        assert!(
            calls.contains(&"create tenant-a aes256-gcm96".to_string()),
            "{calls:?}"
        );
        assert!(calls.contains(&"delete tenant-a".to_string()), "{calls:?}");
        assert!(vault.key("../escape").is_err(), "key names are checked");

        // Another Vault with a key at the same path: the ciphertext is
        // refused (400), which is the key's verdict on it, not the token's.
        let (other_address, _) = serve_transit().await;
        let other = VaultTransitProvider::new(&other_address, "skilj", "test-token");
        assert!(matches!(
            other.unwrap(&new).await,
            Err(encryption::Error::DecryptFailed)
        ));

        // A bad token is a refusal; a provider that isn't there is unavailable.
        let wrong = VaultTransitProvider::new(&address, "skilj", "nope");
        assert!(matches!(
            wrong.wrap(&data_key).await,
            Err(encryption::Error::ProviderRefused { .. })
        ));
        let gone = VaultTransitProvider::new("http://127.0.0.1:1", "skilj", "test-token");
        assert!(matches!(
            gone.versions().await,
            Err(encryption::Error::ProviderUnavailable { .. })
        ));

        // A token file is read for every request (a Vault Agent sink).
        let token_file =
            std::env::temp_dir().join(format!("skilj-vault-token-{}", generate_token_id()));
        std::fs::write(&token_file, "test-token\n").unwrap();
        let agent = VaultTransitProvider::new(&address, "skilj", "unused").token_file(&token_file);
        assert_eq!(agent.unwrap(&new).await.unwrap(), data_key);
        std::fs::remove_file(&token_file).unwrap();
    });
}

// --- the lifecycle, end to end ---

/// A requested rotation with each provider: the rewrap runs in the
/// background, the old version is kept through the grace period, and a
/// rotation asked to retire immediately retires it as soon as the rewrap is
/// done.
async fn rotate_end_to_end(
    url: &str,
    pool: &Pool,
    keys: KeyService,
    raw_unwrap: impl Fn(&WrappedDataKey) -> bool,
) {
    let bc_a = insert_bounded_context(pool).await;
    let bc_b = insert_bounded_context(pool).await;
    let provider = keys.provider_name().to_string();

    // The first start records the version in use.
    let (skilj, _) = builder(url, keys.clone(), quick(Duration::from_secs(3600)))
        .build()
        .await
        .unwrap();
    let rotated = admin_events(pool, "EncryptionMasterKeyRotated").await;
    assert_eq!(rotated.len(), 1);
    assert_eq!(rotated[0]["reason"], "adopted");
    assert_eq!(rotated[0]["version"], 1);
    assert_eq!(rotated[0]["provider"], provider.as_str());

    let sealed_a = seal_for(pool, &bc_a, "1", &keys).await;
    let sealed_b = seal_for(pool, &bc_b, "2", &keys).await;
    seal_for(pool, &bc_a, "forgotten", &keys).await;
    db::destroy_encryption_key(pool, &bc_a, "user", "forgotten", test_now())
        .await
        .unwrap();
    let before = wraps(pool, &bc_a).await;
    assert_eq!(before[0].1.as_deref(), Some(provider.as_str()));
    assert_eq!(before[0].2, Some(1));
    let old_wrap = WrappedDataKey {
        version: 1,
        ciphertext: before[0].3.clone(),
    };

    // A requested rotation, with the grace period: the rewrap runs on its
    // own, and the old version still unwraps afterwards.
    db::rotate_master_key(
        pool,
        keys.provider(),
        None,
        db::Rotation::Rotate(skilj_core::encryption::trail::RotationReason::Requested),
        false,
        Some("superadmin-role"),
        None,
    )
    .await
    .unwrap();
    wait_for_state(pool, &provider, "the rewrap to version 2", |s| {
        s.version == 2 && s.rewrapped_at.is_some()
    })
    .await;
    for bc in [&bc_a, &bc_b] {
        assert!(wraps(pool, bc)
            .await
            .iter()
            .all(|(_, p, v, _)| { p.as_deref() == Some(provider.as_str()) && *v == Some(2) }));
    }
    let rewrapped = admin_events(pool, "EncryptionDataKeysRewrapped").await;
    assert_eq!(rewrapped.len(), 2, "one per bounded context with data keys");
    assert!(rewrapped
        .iter()
        .all(|e| e["version"] == 2 && e["rewrapped"] == 1));
    let rotated = admin_events(pool, "EncryptionMasterKeyRotated").await;
    assert_eq!(rotated[1]["reason"], "requested");
    assert_eq!(rotated[1]["previous_version"], 1);
    assert_eq!(rotated[1]["actor_role_id"], "superadmin-role");
    assert!(raw_unwrap(&old_wrap), "kept through the grace period");
    assert!(admin_events(pool, "EncryptionMasterKeyRetired")
        .await
        .is_empty());

    // What was sealed before still opens: the data keys are the same.
    assert_eq!(
        open_for(pool, &bc_a, "1", &keys, &sealed_a).await,
        "1@example.com"
    );
    // A destroyed key stays destroyed.
    assert!(
        db::get_active_encryption_key(pool, &bc_a, "user", "forgotten")
            .await
            .unwrap()
            .is_none()
    );

    // A leak: rotated again, retired as soon as the rewrap is done.
    db::rotate_master_key(
        pool,
        keys.provider(),
        None,
        db::Rotation::Rotate(skilj_core::encryption::trail::RotationReason::Requested),
        true,
        None,
        None,
    )
    .await
    .unwrap();
    wait_for_state(pool, &provider, "version 3 retiring the ones below", |s| {
        s.version == 3 && s.retired_below == 3
    })
    .await;
    let retired = admin_events(pool, "EncryptionMasterKeyRetired").await;
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0]["below_version"], 3);
    assert!(
        !raw_unwrap(&old_wrap),
        "the old version is unusable at the provider"
    );
    assert_eq!(keys.provider().versions().await.unwrap().min_decryption, 3);
    assert_eq!(
        open_for(pool, &bc_b, "2", &keys, &sealed_b).await,
        "2@example.com"
    );
    skilj.shutdown(Duration::from_secs(10)).await;
}

#[test]
fn a_requested_rotation_with_a_key_file() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_file_rotation_test").await else {
            return;
        };
        let path = temp_key_file();
        let keys =
            KeyService::new(KeyFileProvider::open(&path).unwrap()).with_cache(Duration::ZERO, 0);
        let file = path.clone();
        rotate_end_to_end(&url, &pool, keys, move |wrapped| {
            // Usable while the file still has the version's key.
            let keyring: Value =
                serde_json::from_str(&std::fs::read_to_string(&file).unwrap()).unwrap();
            keyring["min_decryption_version"].as_u64().unwrap() <= u64::from(wrapped.version)
                && keyring["versions"][(wrapped.version - 1) as usize]
                    .get("key")
                    .is_some()
        })
        .await;
        // The retired key is gone from the file, not just disabled.
        let file: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(file["versions"][0].get("key").is_none());
        std::fs::remove_file(&path).unwrap();
    });
}

#[test]
fn a_requested_rotation_with_vault() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_vault_rotation_test").await else {
            return;
        };
        let (address, fake) = serve_transit().await;
        let keys = KeyService::new(VaultTransitProvider::new(&address, "skilj", "test-token"))
            .with_cache(Duration::ZERO, 0);
        let min_decryption = fake.clone();
        rotate_end_to_end(&url, &pool, keys, move |wrapped| {
            wrapped.version >= min_decryption.lock().unwrap().min_decryption
        })
        .await;
        // The rewrap went through Vault's own rewrap: no data key came back.
        let calls = fake.lock().unwrap().calls.clone();
        assert!(calls.iter().any(|c| c == "rewrap"));
        assert!(calls.iter().any(|c| c == "config"));
    });
}

#[test]
fn a_scheduled_rotation_runs_on_its_own_and_retires_after_the_grace_period() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_scheduled_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let keys = KeyService::new(InMemoryKeyProvider::new());
        seal_for(&pool, &bc, "1", &keys).await;
        let (_skilj, _) = builder(
            &url,
            keys.clone(),
            quick(Duration::ZERO).every(Duration::from_millis(300)),
        )
        .build()
        .await
        .unwrap();
        wait_for_state(
            &pool,
            keys.provider_name(),
            "a scheduled rotation, rewrapped and retired",
            |s| s.version >= 2 && s.retired_below >= 2,
        )
        .await;
        let rotated = admin_events(&pool, "EncryptionMasterKeyRotated").await;
        assert!(rotated.iter().any(|e| e["reason"] == "scheduled"));
        assert!(wraps(&pool, &bc)
            .await
            .iter()
            .all(|(_, _, v, _)| v.unwrap_or(0) >= 2));
    });
}

#[test]
fn a_rotation_made_at_the_provider_directly_is_recorded_and_followed() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_external_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let keys = KeyService::new(InMemoryKeyProvider::new());
        seal_for(&pool, &bc, "1", &keys).await;
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::from_secs(3600)))
            .build()
            .await
            .unwrap();
        // `vault write -f transit/keys/skilj/rotate`, say.
        keys.provider().rotate().await.unwrap();
        wait_for_state(
            &pool,
            keys.provider_name(),
            "the external rotation's rewrap",
            |s| s.version == 2 && s.rewrapped_at.is_some(),
        )
        .await;
        let rotated = admin_events(&pool, "EncryptionMasterKeyRotated").await;
        assert_eq!(rotated.last().unwrap()["reason"], "external");
        assert_eq!(wraps(&pool, &bc).await[0].2, Some(2));
    });
}

#[test]
fn data_keys_from_before_key_providers_need_their_key_and_are_moved_to_the_provider() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_legacy_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        // A data key as a version before key providers stored it.
        let legacy = EncryptionMasterKey::from_bytes([11u8; 32]);
        let (data_key, wrapped, nonce) = encryption::generate_and_wrap_data_key(&legacy);
        let sealed = encryption::encrypt_leaf(&data_key, "1@example.com");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO \"bc_{bc}\".encryption_keys \
             (subject_key, subject_value, status, created_at, wrapped_key, wrap_nonce) \
             VALUES ('user', '1', 'active', now(), $1, $2)"
        )))
        .bind(&wrapped)
        .bind(&nonce)
        .execute(&pool)
        .await
        .unwrap();

        // A newer one that is corrupt: it must not read as the wrong key.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO \"bc_{bc}\".encryption_keys \
             (subject_key, subject_value, status, created_at, wrapped_key, wrap_nonce) \
             VALUES ('user', '2', 'active', now(), '\\x0001'::bytea, $1)"
        )))
        .bind(&nonce)
        .execute(&pool)
        .await
        .unwrap();

        let provider = InMemoryKeyProvider::new();
        let keys = || KeyService::new(provider.clone());
        assert_eq!(
            refused(builder(&url, keys(), quick(Duration::ZERO))).await,
            "encryption_legacy_master_key_required"
        );
        let wrong = match builder(&url, keys(), quick(Duration::ZERO))
            .legacy_encryption_master_key(EncryptionMasterKey::from_bytes([12u8; 32]))
            .build()
            .await
        {
            Ok(_) => panic!("the wrong legacy key was accepted"),
            Err(err) => err.to_string(),
        };
        assert!(wrong.contains(&bc), "names the bounded context: {wrong}");
        assert_eq!(
            refused(
                builder(&url, keys(), quick(Duration::ZERO))
                    .legacy_encryption_master_key(EncryptionMasterKey::from_bytes([12u8; 32]))
            )
            .await,
            "encryption_legacy_master_key_changed"
        );
        let (_skilj, _) = builder(&url, keys(), quick(Duration::ZERO))
            .legacy_encryption_master_key(EncryptionMasterKey::from_bytes([11u8; 32]))
            .build()
            .await
            .unwrap();
        wait_for_state(&pool, provider.name(), "the legacy data keys moved", |s| {
            s.rewrapped_at.is_some()
        })
        .await;
        assert_eq!(
            wraps(&pool, &bc).await[0].1.as_deref(),
            Some(provider.name())
        );
        let rewrapped = events_about(&pool, "EncryptionDataKeysRewrapped", &bc).await;
        assert_eq!(
            (
                rewrapped[0]["rewrapped"].clone(),
                rewrapped[0]["unreadable"].clone()
            ),
            (json!(1), json!(1))
        );
        // Readable now without the legacy key.
        assert_eq!(
            open_for(&pool, &bc, "1", &keys(), &sealed).await,
            "1@example.com"
        );
    });
}

#[test]
fn data_keys_wrapped_by_another_provider_refuse_startup() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_provider_changed_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let path = temp_key_file();
        let file_keys = KeyService::new(KeyFileProvider::open(&path).unwrap());
        seal_for(&pool, &bc, "1", &file_keys).await;
        assert_eq!(
            refused(builder(
                &url,
                KeyService::new(InMemoryKeyProvider::new()),
                quick(Duration::ZERO)
            ))
            .await,
            "encryption_key_provider_changed"
        );
        std::fs::remove_file(&path).unwrap();
    });
}

#[test]
fn a_legacy_key_needs_a_provider() {
    runtime().block_on(async {
        let err = match Skilj::builder("postgres://unused".to_string())
            .legacy_encryption_master_key(EncryptionMasterKey::from_bytes([1u8; 32]))
            .build()
            .await
        {
            Ok(_) => panic!("a legacy key without a provider was accepted"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("without a key_provider"));
    });
}

// --- the GraphQL surface ---

const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDPHVFsUHiWXSbG
/TCig1cTQHNT6FnoYoZtMEjvDiQArsOL/dFoM9pmGRM9CfEtQGNum4TsimPtgJec
awfdPnW0uJCRlIF9wGmYdh2mYNBKw8jqxwp664Gd5uqH5L6A4pN8bfGO7+2niD6p
8t0cNeyYOd0PusbAEDcpzCUZmr6KQyM5i8/wk5oO98gntp+ZpMjUZabAD6R8DyhM
IZmV645jo5NPJG7zuSz+3dmKkNY0/GXz8YwvZ2swqmmOANRZHHfN1vgP2ycK02WZ
4yihx6EiuQCDseddBw+xit9KSvSq6GwmwnV1qVpMVNlSGGOeVX7v7JQ3z/BNbQ85
5p6s/FjhAgMBAAECggEAFu8fKghLIhNUjOpSbVxv0vDrFFqBQitOyV50ZQxCzlSL
0L+dZZWAVJfoOnUUYLdli0TrVioI4K7Bmw97AnO9IvLhB03TfPJGfxxtMhQ8XFsL
r3u03GGhq7N7OusIcUslm7ys5/AHd+qtTbJX65zJAx49LVW4VmI1SYqSfSBWgway
8uGYaXyCfwuxQ+xB4fQd6llm/+9dqS+U36LVSMWgEmVjceorYFhPVLfuX4A1wHjF
mDl40AwPBqzVbOIzFDMDikk4heFi6wlt6N3LGDtyBUUuzEg5TBhyiirvNvTjW+4V
Z4MZs3tez+IqM0+F4EsgAEQUU12YQxa4lobm8/zgZQKBgQD81FMzymNR6xWhUSwY
4RtkVntfMBOMp1rVGcVyBxOLKxEXF6ctk2rV38krfUI50h/lWzrbpl+zJvEe8D1H
vZjYj28sL3wf0CSnPYUeGANTxrW1dTiz1HVzzChfbAEWj3fsVrlghNcnHBkDDhqz
L/rPEfp//fB0SyLAEAJt87cgFwKBgQDRtjtH1gIkGn5GCS3u0FAbxV+qrUlTvu4t
Di1GcEw32jootQQSMZN1PxEvLuehaBlaASEL2OZzZlQ4q60LV1Jisvd7wqv5EYnG
o+sKtrCS5iXKfkxqTmg+JS7OZazggyvgBnv4GXT0US6/G4nw7C9JaS2jyOvPGIPS
K8dsWDIxxwKBgQCgr4FBxTticPqKUECqf0cdeilm0fNazXJZRcvLMNwm8vQlrQ6/
VJXt4BDG5xEUFovXBShfOVpRTkqo0x7fXYyq9l49wuAsh+kDsYHNIo3azMvny9yB
zmHnerWeD9KROBWLy4J96W+kl6L94hTuFWxd9psyhX4xKx+m2YXxw5d7eQKBgFB2
I86PHOkvRQ2oDfiX8nSFSQxaSk0Yb5fX3aUuBwBS+YeO1E4KuXH9zaEV1QeHwlpX
Ho/GG71hIKVRsSYtzc1Sr0PL0GHSydLuJ4tHxv3F0fAcf0M2bCaT656DQk4t5dKh
ikUJt2baEx59+XH3nLkE4t75gwhFdqZX5775I+EXAoGAfnpHlLZdGW48rl9Cl887
hRDjXDm/gP/ljCrvxxiWselEgaLj2o4NiT28QAfq7KgtOIpAeLAGzIBP6vkE7KFp
nAF+t4gRpooXXSI5oXCBcGI9a26q68UV3iDEmQGiP8kVHOsdzcOKY0qk1ulNAIV4
fU919gnTKorSq3FdV6zGZ8s=
-----END PRIVATE KEY-----
";

const TEST_MODULUS_N: &str = "zx1RbFB4ll0mxv0wooNXE0BzU-hZ6GKGbTBI7w4kAK7Di_3RaDPaZhkTPQnxLUBjbpuE7Ipj7YCXnGsH3T51tLiQkZSBfcBpmHYdpmDQSsPI6scKeuuBnebqh-S-gOKTfG3xju_tp4g-qfLdHDXsmDndD7rGwBA3KcwlGZq-ikMjOYvP8JOaDvfIJ7afmaTI1GWmwA-kfA8oTCGZleuOY6OTTyRu87ks_t3ZipDWNPxl8_GML2drMKppjgDUWRx3zdb4D9snCtNlmeMoocehIrkAg7HnXQcPsYrfSkr0quhsJsJ1dalaTFTZUhhjnlV-7-yUN8_wTW0POeaerPxY4Q";
const TEST_EXPONENT_E: &str = "AQAB";
const TEST_KID: &str = "test-key-1";
const TEST_ISSUER: &str = "https://idp.example.test/";
/// The `aud` this deployment's tokens carry - `IdpConfig` requires one.
const TEST_AUDIENCE: &str = "skilj-test-client";

// --- JWKS test server + JWT signing ---

async fn serve_jwks() -> String {
    let jwks = json!({
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": TEST_KID,
            "n": TEST_MODULUS_N,
            "e": TEST_EXPONENT_E,
        }]
    });
    let app = axum::Router::new().route(
        "/jwks.json",
        axum::routing::get(move || {
            let jwks = jwks.clone();
            async move { axum::Json(jwks) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind an ephemeral port for the JWKS test server");
    let addr = listener
        .local_addr()
        .expect("a bound listener always has a local address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("the JWKS test server stopped unexpectedly");
    });
    format!("http://{addr}/jwks.json")
}

fn sign_jwt(subject: &str) -> String {
    let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(TEST_KID.to_string());
    let claims = json!({
        "sub": subject,
        "iss": TEST_ISSUER,
        "aud": TEST_AUDIENCE,
        "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
    });
    let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes())
        .expect("the test private key PEM is well-formed");
    jsonwebtoken::encode(&header, &claims, &key).expect("signing a well-formed JWT never fails")
}

async fn graphql_request(
    router: &axum::Router,
    jwt: &str,
    query: &str,
    variables: serde_json::Value,
) -> serde_json::Value {
    let request = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("authorization", format!("Bearer {jwt}"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "query": query, "variables": variables }).to_string(),
        ))
        .unwrap();

    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

const ROTATE: &str = "mutation($now: Boolean, $bc: String) { \
    rotateMasterKey(boundedContext: $bc, retireImmediately: $now) { \
        provider version rewrapPending retireImmediately retiredBelow \
    } }";
const STATE: &str =
    "{ masterKeys { provider key version rewrapPending rewrappedAt retiredBelow boundedContexts } }";
const ASSIGN: &str = "mutation($bc: String!, $key: String, $create: Boolean) { \
    assignMasterKey(boundedContext: $bc, key: $key, create: $create) { \
        provider key version createdBySkilj boundedContexts \
    } }";

#[test]
fn a_superadmin_rotates_over_graphql_and_reads_where_it_stands() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_graphql_test").await else {
            return;
        };
        let jwks = serve_jwks().await;
        let superadmin = skilj_core::access_control::Role {
            id: generate_token_id(),
            external_subject: format!("root_{}", generate_token_id()),
            name: "Root".to_string(),
            superadmin: true,
            status: skilj_core::access_control::RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &superadmin).await.unwrap();
        let other = skilj_core::access_control::Role {
            id: generate_token_id(),
            external_subject: format!("other_{}", generate_token_id()),
            name: "Other".to_string(),
            superadmin: false,
            status: skilj_core::access_control::RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &other).await.unwrap();
        let bc = insert_bounded_context(&pool).await;
        let keys = KeyService::new(InMemoryKeyProvider::new());
        seal_for(&pool, &bc, "1", &keys).await;

        let (skilj, _) = builder(&url, keys.clone(), quick(Duration::from_secs(3600)))
            .identity_provider(IdpConfig::new(
                jwks.parse().unwrap(),
                TEST_ISSUER,
                TEST_AUDIENCE,
                SigningAlgorithm::Rs256,
            ))
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();
        let root = sign_jwt(&superadmin.external_subject);

        let state = graphql_request(&router, &root, STATE, json!({})).await;
        assert_eq!(state["data"]["masterKeys"][0]["version"], 1, "{state}");
        assert_eq!(state["data"]["masterKeys"][0]["provider"], "in-memory");

        // Only a superadmin.
        let refused = graphql_request(
            &router,
            &sign_jwt(&other.external_subject),
            ROTATE,
            json!({}),
        )
        .await;
        assert!(refused["errors"].is_array(), "{refused}");
        assert_eq!(keys.provider().versions().await.unwrap().latest, 1);

        // A name that is no bounded context is refused, not read as the
        // default key's.
        for typo in ["typo", ""] {
            let refused = graphql_request(&router, &root, ROTATE, json!({ "bc": typo })).await;
            assert!(refused["errors"].is_array(), "{refused}");
        }
        assert_eq!(keys.provider().versions().await.unwrap().latest, 1);

        let rotated = graphql_request(&router, &root, ROTATE, json!({ "now": true })).await;
        let rotated = &rotated["data"]["rotateMasterKey"];
        assert_eq!(rotated["version"], 2, "{rotated}");
        assert_eq!(rotated["rewrapPending"], true);
        assert_eq!(rotated["retireImmediately"], true);
        let event = admin_events(&pool, "EncryptionMasterKeyRotated").await;
        assert_eq!(
            event.last().unwrap()["actor_role_id"],
            superadmin.id.as_str()
        );

        wait_for_state(
            &pool,
            keys.provider_name(),
            "the retirement after the GraphQL rotation",
            |s| s.retired_below == 2,
        )
        .await;
        let state = graphql_request(&router, &root, STATE, json!({})).await;
        let state = &state["data"]["masterKeys"][0];
        assert_eq!(state["rewrapPending"], false);
        assert!(state["rewrappedAt"].is_string());
        assert_eq!(state["retiredBelow"], 2);

        // Its own key, then where every key stands.
        let assigned = graphql_request(
            &router,
            &root,
            ASSIGN,
            json!({ "bc": bc, "key": "own-key", "create": true }),
        )
        .await;
        let assigned = &assigned["data"]["assignMasterKey"];
        assert_eq!(assigned["provider"], "in-memory:own-key", "{assigned}");
        assert_eq!(assigned["createdBySkilj"], true);
        assert_eq!(assigned["boundedContexts"], json!([bc]));
        let all = graphql_request(&router, &root, STATE, json!({})).await;
        let all = all["data"]["masterKeys"].as_array().unwrap().clone();
        assert_eq!(all.len(), 2);
        assert!(!all[0]["boundedContexts"]
            .as_array()
            .unwrap()
            .contains(&json!(bc)));
    });
}

// --- bounded contexts with keys of their own ---

/// Every event of type `name` about `bounded_context`.
async fn events_about(pool: &Pool, name: &str, bounded_context: &str) -> Vec<Value> {
    admin_events(pool, name)
        .await
        .into_iter()
        .filter(|e| e["bounded_context"] == bounded_context)
        .collect()
}

/// Waits until `check` holds, polling.
async fn wait_until<F, Fut>(pool: &Pool, what: &str, check: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..600 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}\n{}", dump(pool).await);
}

/// Every key's state and every admin event's type, for a timeout's message.
async fn dump(pool: &Pool) -> String {
    let events: Vec<String> = db::list_events_for_bounded_context(pool, ADMIN_BOUNDED_CONTEXT_NAME)
        .await
        .map(|events| {
            events
                .into_iter()
                .map(|e| format!("{} {}", e.event_type.name, e.payload))
                .collect()
        })
        .unwrap_or_default();
    format!(
        "keys: {:#?}\nevents: {:#?}",
        db::master_key_states(pool).await.unwrap_or_default(),
        events
    )
}

#[test]
fn a_bounded_context_with_its_own_key_is_moved_rotated_and_its_key_deleted_with_it() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_own_key_test").await else {
            return;
        };
        let tenant = insert_bounded_context(&pool).await;
        let other = insert_bounded_context(&pool).await;
        let provider = InMemoryKeyProvider::new();
        let keys = KeyService::new(provider.clone());
        let sealed = seal_for(&pool, &tenant, "1", &keys).await;
        seal_for(&pool, &other, "2", &keys).await;
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::ZERO))
            .build()
            .await
            .unwrap();

        // Its own key, created by skilj: its data keys move there.
        let assigned =
            db::assign_encryption_key(&pool, &keys, &tenant, Some("tenant-a"), true, Some("root"))
                .await
                .unwrap();
        assert_eq!(assigned.provider, "in-memory:tenant-a");
        assert!(assigned.created_by_skilj);
        wait_for_state(
            &pool,
            "in-memory:tenant-a",
            "the move to the tenant's key",
            |s| s.rewrapped_at.is_some(),
        )
        .await;
        assert_eq!(
            wraps(&pool, &tenant).await[0].1.as_deref(),
            Some("in-memory:tenant-a")
        );
        assert_eq!(
            wraps(&pool, &other).await[0].1.as_deref(),
            Some("in-memory")
        );
        let assigned_event = events_about(&pool, "EncryptionKeyAssigned", &tenant).await;
        assert_eq!(assigned_event.len(), 1);
        assert_eq!(assigned_event[0]["previous_provider"], "in-memory");
        assert_eq!(assigned_event[0]["created"], true);
        // New data keys of the tenant are wrapped by its key.
        seal_for(&pool, &tenant, "3", &keys).await;
        assert!(wraps(&pool, &tenant)
            .await
            .iter()
            .all(|(_, p, _, _)| p.as_deref() == Some("in-memory:tenant-a")));

        // Rotating the tenant's key leaves the default key alone.
        let tenant_key = keys.provider_for(Some("tenant-a")).unwrap();
        db::rotate_master_key(
            &pool,
            tenant_key.as_ref(),
            Some("tenant-a"),
            db::Rotation::Rotate(skilj_core::encryption::trail::RotationReason::Requested),
            true,
            None,
            None,
        )
        .await
        .unwrap();
        wait_for_state(
            &pool,
            "in-memory:tenant-a",
            "the tenant key's retirement",
            |s| s.version == 2 && s.retired_below == 2,
        )
        .await;
        assert_eq!(
            db::master_key_state(&pool, "in-memory")
                .await
                .unwrap()
                .unwrap()
                .version,
            1
        );
        assert_eq!(wraps(&pool, &other).await[0].2, Some(1));

        // An instance that has never used the tenant's key still reads it.
        let fresh = KeyService::new(provider.clone());
        assert_eq!(
            open_for(&pool, &tenant, "1", &fresh, &sealed).await,
            "1@example.com"
        );

        // Deleting the bounded context deletes its key after the grace period.
        db::hard_delete_bounded_context(&pool, &tenant)
            .await
            .unwrap();
        wait_until(&pool, "the tenant key's deletion", || async {
            !admin_events(&pool, "EncryptionKeyDeleted").await.is_empty()
        })
        .await;
        let deleted = admin_events(&pool, "EncryptionKeyDeleted").await;
        assert_eq!(deleted[0]["provider"], "in-memory:tenant-a");
        assert_eq!(deleted[0]["deleted_at_provider"], true);
        assert!(tenant_key.versions().await.is_err(), "gone at the provider");
        assert!(db::master_key_state(&pool, "in-memory:tenant-a")
            .await
            .unwrap()
            .is_none());
    });
}

#[test]
fn a_key_a_superadmin_brought_is_let_go_of_but_not_deleted() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_byok_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let provider = InMemoryKeyProvider::new();
        let keys = KeyService::new(provider.clone());
        seal_for(&pool, &bc, "1", &keys).await;
        // The customer's key, made outside skilj.
        let theirs = provider.key("customer-key").unwrap();
        theirs.create().await.unwrap();
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::ZERO))
            .build()
            .await
            .unwrap();

        // A key that doesn't exist is refused, and nothing is recorded.
        let missing = db::assign_encryption_key(&pool, &keys, &bc, Some("nope"), false, None).await;
        assert!(missing.is_err());
        assert!(db::bounded_context_key(&pool, &bc).await.unwrap().is_none());

        // Asked to create it, skilj finds it there and claims nothing.
        let assigned =
            db::assign_encryption_key(&pool, &keys, &bc, Some("customer-key"), true, None)
                .await
                .unwrap();
        assert!(!assigned.created_by_skilj);
        let event = events_about(&pool, "EncryptionKeyAssigned", &bc).await;
        assert_eq!(event[0]["created"], false);
        wait_for_state(
            &pool,
            "in-memory:customer-key",
            "the move to their key",
            |s| s.rewrapped_at.is_some(),
        )
        .await;
        assert_eq!(
            wraps(&pool, &bc).await[0].1.as_deref(),
            Some("in-memory:customer-key")
        );

        // Back to the default key: the data keys move back, and their key is
        // only let go of.
        db::assign_encryption_key(&pool, &keys, &bc, None, false, None)
            .await
            .unwrap();
        wait_until(&pool, "their key let go of", || async {
            !admin_events(&pool, "EncryptionKeyDeleted").await.is_empty()
        })
        .await;
        let deleted = admin_events(&pool, "EncryptionKeyDeleted").await;
        assert_eq!(deleted[0]["deleted_at_provider"], false);
        assert_eq!(wraps(&pool, &bc).await[0].1.as_deref(), Some("in-memory"));
        assert!(
            theirs.versions().await.is_ok(),
            "still there at the provider"
        );
    });
}

#[test]
fn unreadable_data_keys_are_marked_once_and_one_event_covers_every_batch() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_unreadable_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let keys = KeyService::new(InMemoryKeyProvider::new());
        // More than one rewrap batch.
        for subject in 0..250 {
            seal_for(&pool, &bc, &format!("s{subject:03}"), &keys).await;
        }
        // One corrupt data key.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{bc}\".encryption_keys SET wrapped_key = '\\x00010203'::bytea \
             WHERE subject_value = 's100'"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::ZERO))
            .build()
            .await
            .unwrap();
        db::rotate_master_key(
            &pool,
            keys.provider(),
            None,
            db::Rotation::Rotate(skilj_core::encryption::trail::RotationReason::Requested),
            false,
            None,
            None,
        )
        .await
        .unwrap();
        // The unreadable key holds up neither the rewrap nor the retirement.
        wait_for_state(&pool, keys.provider_name(), "the retirement", |s| {
            s.version == 2 && s.retired_below == 2
        })
        .await;
        // Some more checks pass without anything new being recorded.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let rewrapped = events_about(&pool, "EncryptionDataKeysRewrapped", &bc).await;
        assert_eq!(rewrapped.len(), 1, "{rewrapped:?}");
        assert_eq!(rewrapped[0]["rewrapped"], 249);
        assert_eq!(rewrapped[0]["unreadable"], 1);
        let marked: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(sqlx::AssertSqlSafe(
            format!(
                "SELECT unreadable_since FROM \"bc_{bc}\".encryption_keys WHERE subject_value = 's100'"
            ),
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(marked.is_some());
    });
}

#[test]
fn a_data_key_wrapped_under_an_old_version_after_the_rewrap_holds_up_the_retirement() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_straggler_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let keys = KeyService::new(InMemoryKeyProvider::new());
        seal_for(&pool, &bc, "1", &keys).await;
        let old = wraps(&pool, &bc).await[0].3.clone();
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::from_secs(3600)))
            .build()
            .await
            .unwrap();
        db::rotate_master_key(
            &pool,
            keys.provider(),
            None,
            db::Rotation::Rotate(skilj_core::encryption::trail::RotationReason::Requested),
            false,
            None,
            None,
        )
        .await
        .unwrap();
        wait_for_state(&pool, keys.provider_name(), "the rewrap", |s| {
            s.rewrapped_at.is_some()
        })
        .await;
        // An instance that hadn't seen the rotation wrapped a data key under
        // version 1 after the rewrap passed it - and the grace period is over.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{bc}\".encryption_keys SET wrapped_key = $1, master_key_version = 1"
        )))
        .bind(&old)
        .execute(&pool)
        .await
        .unwrap();
        let aged = "UPDATE encryption_master_keys SET rewrapped_at = now() - interval '2 hours'";
        sqlx::query(aged).execute(&pool).await.unwrap();
        // The retirement check finds it and sends the key back to the
        // rewrap; the rewrap's end restarts the grace period, so nothing is
        // retired yet.
        wait_until(&pool, "the straggler rewrapped", || async {
            events_about(&pool, "EncryptionDataKeysRewrapped", &bc)
                .await
                .len()
                == 2
        })
        .await;
        wait_for_state(&pool, keys.provider_name(), "the rewrap done again", |s| {
            s.rewrapped_at
                .is_some_and(|at| at > chrono::Utc::now() - chrono::Duration::minutes(5))
        })
        .await;
        assert_eq!(wraps(&pool, &bc).await[0].2, Some(2));
        let state = db::master_key_state(&pool, keys.provider_name())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.retired_below, 1, "postponed by a new grace period");
        // Once that one is over too, the old version retires.
        sqlx::query(aged).execute(&pool).await.unwrap();
        wait_for_state(&pool, keys.provider_name(), "the retirement", |s| {
            s.retired_below == 2
        })
        .await;
    });
}

#[test]
fn another_key_under_the_same_name_refuses_startup_and_marks_are_tried_again() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_material_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let path = temp_key_file();
        let keys = KeyService::new(KeyFileProvider::open(&path).unwrap());
        let sealed = seal_for(&pool, &bc, "1", &keys).await;
        seal_for(&pool, &bc, "2", &keys).await;

        // The key file lost and created again at the same path: the same
        // name, other key material. Refused before anything is marked.
        let original = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let recreated = KeyService::new(KeyFileProvider::open(&path).unwrap());
        assert_eq!(
            refused(builder(&url, recreated, quick(Duration::ZERO))).await,
            "encryption_key_material_changed"
        );
        std::fs::write(&path, &original).unwrap();

        let mark = |subject: &'static str| {
            let pool = pool.clone();
            let bc = bc.clone();
            async move {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE \"bc_{bc}\".encryption_keys SET unreadable_since = now() \
                     WHERE subject_value = '{subject}'"
                )))
                .execute(&pool)
                .await
                .unwrap();
            }
        };
        let marked = |subject: &'static str| {
            let pool = pool.clone();
            let bc = bc.clone();
            async move {
                sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(sqlx::AssertSqlSafe(
                    format!(
                        "SELECT unreadable_since FROM \"bc_{bc}\".encryption_keys \
                         WHERE subject_value = '{subject}'"
                    ),
                ))
                .fetch_one(&pool)
                .await
                .unwrap()
                .is_some()
            }
        };
        // A data key marked unreadable that unwraps after all - its key was
        // put right - loses the mark when read...
        mark("1").await;
        assert_eq!(
            open_for(&pool, &bc, "1", &keys, &sealed).await,
            "1@example.com"
        );
        assert!(!marked("1").await);

        // ...and one never read is tried again by the next rotation.
        mark("2").await;
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::ZERO))
            .build()
            .await
            .unwrap();
        db::rotate_master_key(
            &pool,
            keys.provider(),
            None,
            db::Rotation::Rotate(skilj_core::encryption::trail::RotationReason::Requested),
            false,
            None,
            None,
        )
        .await
        .unwrap();
        wait_for_state(&pool, keys.provider_name(), "the retirement", |s| {
            s.retired_below == 2
        })
        .await;
        assert!(!marked("2").await);
        assert!(wraps(&pool, &bc)
            .await
            .iter()
            .all(|(_, _, v, _)| *v == Some(2)));
        std::fs::remove_file(&path).unwrap();
    });
}

#[test]
fn a_key_is_not_deleted_while_a_data_key_is_wrapped_by_it() {
    runtime().block_on(async {
        let Some((_url, pool)) = test_db("skilj_master_key_delete_race_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        // The admin context, for the events.
        let (_skilj, _) = builder(
            &_url,
            KeyService::new(InMemoryKeyProvider::new()),
            quick(Duration::from_secs(3600)),
        )
        .build()
        .await
        .unwrap();
        _skilj.shutdown(Duration::from_secs(10)).await;
        let provider = InMemoryKeyProvider::new();
        let keys = KeyService::new(provider.clone());
        db::assign_encryption_key(&pool, &keys, &bc, Some("tenant"), true, None)
            .await
            .unwrap();
        seal_for(&pool, &bc, "1", &keys).await;
        assert_eq!(
            wraps(&pool, &bc).await[0].1.as_deref(),
            Some("in-memory:tenant")
        );

        // As if the context had just been given the default key back and
        // the deletion came due while a data key is still wrapped by its
        // own key - by a rewrap or a provisioning that read the old
        // assignment.
        sqlx::query("DELETE FROM encryption_key_assignments")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE encryption_master_keys SET delete_after = now() - interval '1 minute' \
             WHERE provider = 'in-memory:tenant'",
        )
        .execute(&pool)
        .await
        .unwrap();
        let tenant = keys.provider_for(Some("tenant")).unwrap();
        assert!(!db::delete_unused_key(&pool, &keys, tenant.as_ref())
            .await
            .unwrap());
        assert!(tenant.versions().await.is_ok(), "still there");
        let state = db::master_key_state(&pool, "in-memory:tenant")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.delete_after, None, "its deletion starts over");
    });
}

#[test]
fn zero_intervals_are_refused() {
    runtime().block_on(async {
        for rotation in [
            MasterKeyRotation::new().check_interval(Duration::ZERO),
            MasterKeyRotation::new().every(Duration::ZERO),
        ] {
            let err = match Skilj::builder("postgres://unused".to_string())
                .key_provider(InMemoryKeyProvider::new())
                .master_key_rotation(rotation)
                .build()
                .await
            {
                Ok(_) => panic!("a zero interval was accepted"),
                Err(err) => err,
            };
            assert!(err.to_string().contains("more than zero"), "{err}");
        }
    });
}

#[test]
fn another_vault_under_the_same_key_path_refuses_startup_once_checked_per_key() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_vault_material_test").await else {
            return;
        };
        let (address, fake) = serve_transit().await;
        let vault = || {
            KeyService::new(VaultTransitProvider::new(&address, "skilj", "test-token"))
                .with_cache(Duration::ZERO, 0)
        };
        let mut contexts = Vec::new();
        for subject in ["1", "2", "3"] {
            let bc = insert_bounded_context(&pool).await;
            seal_for(&pool, &bc, subject, &vault()).await;
            contexts.push(bc);
        }

        // Healthy: one key, three contexts, one startup probe.
        let before = fake
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|c| *c == "decrypt")
            .count();
        let (skilj, _) = builder(&url, vault(), quick(Duration::from_secs(3600)))
            .build()
            .await
            .unwrap();
        skilj.shutdown(Duration::from_secs(10)).await;
        let after = fake
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|c| *c == "decrypt")
            .count();
        assert_eq!(
            after - before,
            1,
            "the key is checked once, not per context"
        );

        // The default key's own name as a context's key: refused, it would
        // be one key under two lifecycles.
        assert!(db::assign_encryption_key(
            &pool,
            &vault(),
            &contexts[0],
            Some("skilj"),
            false,
            None
        )
        .await
        .is_err());

        // Another Vault with a key at the same path: refused.
        let (other_address, _) = serve_transit().await;
        let other = KeyService::new(VaultTransitProvider::new(
            &other_address,
            "skilj",
            "test-token",
        ));
        assert_eq!(
            refused(builder(&url, other, quick(Duration::from_secs(3600)))).await,
            "encryption_key_material_changed"
        );

        // Vault unreachable: no verdict, the start goes on.
        let down = KeyService::new(VaultTransitProvider::new(
            "http://127.0.0.1:1",
            "skilj",
            "t",
        ));
        let (skilj, _) = builder(&url, down, quick(Duration::from_secs(3600)))
            .build()
            .await
            .unwrap();
        skilj.shutdown(Duration::from_secs(10)).await;
        let _ = contexts;
    });
}

#[test]
fn a_deletion_waits_for_a_provisioning_holding_the_key_and_then_finds_its_data_key() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_interleaving_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let (skilj, _) = builder(
            &url,
            KeyService::new(InMemoryKeyProvider::new()),
            quick(Duration::from_secs(3600)),
        )
        .build()
        .await
        .unwrap();
        skilj.shutdown(Duration::from_secs(10)).await;
        let keys = KeyService::new(InMemoryKeyProvider::new());
        db::assign_encryption_key(&pool, &keys, &bc, Some("tenant"), true, None)
            .await
            .unwrap();
        // The context given the default key back, and the deletion due.
        sqlx::query("DELETE FROM encryption_key_assignments")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE encryption_master_keys SET delete_after = now() - interval '1 minute' \
             WHERE provider = 'in-memory:tenant'",
        )
        .execute(&pool)
        .await
        .unwrap();

        // A provisioning that read the old assignment holds the key's row,
        // as its insert does, and has not committed yet.
        let tenant = keys.provider_for(Some("tenant")).unwrap();
        let wrapped = tenant.wrap(&[7u8; 32]).await.unwrap();
        let mut provisioning = pool.begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "WITH kept AS (SELECT 1 FROM encryption_master_keys \
                 WHERE provider = 'in-memory:tenant' FOR SHARE) \
             INSERT INTO \"bc_{bc}\".encryption_keys \
             (subject_key, subject_value, status, created_at, wrapped_key, key_provider, \
              master_key_version) \
             SELECT 'user', '1', 'active', now(), $1, 'in-memory:tenant', 1 FROM kept"
        )))
        .bind(&wrapped.ciphertext)
        .execute(&mut *provisioning)
        .await
        .unwrap();

        // The deletion waits for it...
        let deletion = {
            let pool = pool.clone();
            let keys = keys.clone();
            let tenant = Arc::clone(&tenant);
            tokio::spawn(async move { db::delete_unused_key(&pool, &keys, tenant.as_ref()).await })
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!deletion.is_finished(), "waits for the provisioning");
        // ...and once it commits, sees its data key and keeps the key.
        provisioning.commit().await.unwrap();
        assert!(!deletion.await.unwrap().unwrap());
        assert!(tenant.versions().await.is_ok(), "the key is still there");
    });
}

#[test]
fn a_key_is_not_retired_while_a_context_moving_off_it_still_has_data_keys_under_it() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_moving_retire_test").await else {
            return;
        };
        let moving = insert_bounded_context(&pool).await;
        let staying = insert_bounded_context(&pool).await;
        let provider = InMemoryKeyProvider::new();
        let keys = KeyService::new(provider.clone()).with_cache(Duration::ZERO, 0);
        let sealed = seal_for(&pool, &moving, "1", &keys).await;
        seal_for(&pool, &staying, "2", &keys).await;
        // An instance that checks once, at its start, and not again: it sets
        // up the admin context, and nothing moves while the scene is set.
        let (setup, _) = builder(
            &url,
            keys.clone(),
            quick(Duration::ZERO).check_interval(Duration::from_secs(3600)),
        )
        .build()
        .await
        .unwrap();
        setup.shutdown(Duration::from_secs(10)).await;

        // The context is given its own key, which then can't be reached:
        // its data keys stay under the default key's version 1.
        db::assign_encryption_key(&pool, &keys, &moving, Some("tenant"), true, None)
            .await
            .unwrap();
        provider.key("tenant").unwrap().delete().await.unwrap();

        // A leak response on the default key: rotate and retire at once.
        db::rotate_master_key(
            &pool,
            keys.provider(),
            None,
            db::Rotation::Rotate(skilj_core::encryption::trail::RotationReason::Requested),
            true,
            None,
            None,
        )
        .await
        .unwrap();
        // Only now an instance that checks often.
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::ZERO))
            .build()
            .await
            .unwrap();
        wait_for_state(
            &pool,
            keys.provider_name(),
            "the default key's rewrap",
            |s| s.version == 2 && s.rewrapped_at.is_some(),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Version 1 still wraps the moving context's data key, so it is not
        // retired, and the data key still opens.
        let state = db::master_key_state(&pool, keys.provider_name())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            state.retired_below, 1,
            "retired under a context still on it"
        );
        assert_eq!(
            open_for(&pool, &moving, "1", &keys, &sealed).await,
            "1@example.com"
        );
    });
}

#[test]
fn one_bad_data_key_in_a_vault_batch_is_marked_and_the_rest_rewrapped() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_vault_partial_test").await else {
            return;
        };
        let (address, _) = serve_transit().await;
        let keys = KeyService::new(VaultTransitProvider::new(&address, "skilj", "test-token"))
            .with_cache(Duration::ZERO, 0);
        let bc = insert_bounded_context(&pool).await;
        for subject in ["a", "b", "c"] {
            seal_for(&pool, &bc, subject, &keys).await;
        }
        // One corrupt ciphertext among them, and then one where every item
        // fails: Vault answers 400 to both batches.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{bc}\".encryption_keys SET wrapped_key = convert_to('vault:v1:x.00', 'UTF8') \
             WHERE subject_value = 'b'"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::ZERO))
            .build()
            .await
            .unwrap();
        db::rotate_master_key(
            &pool,
            keys.provider(),
            None,
            db::Rotation::Rotate(skilj_core::encryption::trail::RotationReason::Requested),
            false,
            None,
            None,
        )
        .await
        .unwrap();
        wait_for_state(&pool, keys.provider_name(), "the retirement", |s| {
            s.version == 2 && s.retired_below == 2
        })
        .await;
        let rewrapped = events_about(&pool, "EncryptionDataKeysRewrapped", &bc).await;
        assert_eq!(rewrapped.len(), 1, "{rewrapped:?}");
        assert_eq!((rewrapped[0]["rewrapped"].clone(), rewrapped[0]["unreadable"].clone()), (json!(2), json!(1)));
    });
}

/// A provider whose `wrap` waits until released: a slow key service.
#[derive(Clone)]
struct Slow {
    inner: InMemoryKeyProvider,
    gate: Arc<tokio::sync::Semaphore>,
}

impl KeyProvider for Slow {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn wrap<'a>(
        &'a self,
        data_key: &'a [u8; 32],
    ) -> skilj::BoxFuture<'a, Result<WrappedDataKey, encryption::Error>> {
        Box::pin(async move {
            let _permit = self.gate.acquire().await.unwrap();
            self.inner.wrap(data_key).await
        })
    }
    fn unwrap<'a>(
        &'a self,
        wrapped: &'a WrappedDataKey,
    ) -> skilj::BoxFuture<'a, Result<[u8; 32], encryption::Error>> {
        self.inner.unwrap(wrapped)
    }
    fn versions(&self) -> skilj::BoxFuture<'_, Result<skilj::KeyVersions, encryption::Error>> {
        self.inner.versions()
    }
    fn rotate(&self) -> skilj::BoxFuture<'_, Result<u32, encryption::Error>> {
        self.inner.rotate()
    }
    fn retire_below(&self, version: u32) -> skilj::BoxFuture<'_, Result<(), encryption::Error>> {
        self.inner.retire_below(version)
    }
    fn key(&self, key: &str) -> Result<Arc<dyn KeyProvider>, encryption::Error> {
        self.inner.key(key)
    }
    fn create(&self) -> skilj::BoxFuture<'_, Result<bool, encryption::Error>> {
        self.inner.create()
    }
    fn delete(&self) -> skilj::BoxFuture<'_, Result<(), encryption::Error>> {
        self.inner.delete()
    }
}

#[test]
fn a_slow_key_provider_does_not_hold_pool_connections() {
    runtime().block_on(async {
        let Some((url, setup)) = test_db("skilj_master_key_slow_provider_test").await else {
            return;
        };
        let bc = insert_bounded_context(&setup).await;
        let pool = skilj_core::db::PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(2))
            .connect(&url)
            .await
            .unwrap();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let keys = KeyService::new(Slow {
            inner: InMemoryKeyProvider::new(),
            gate: Arc::clone(&gate),
        });
        // Four provisionings, each waiting on the provider.
        let waiting: Vec<_> = (0..4)
            .map(|i| {
                let pool = pool.clone();
                let keys = keys.clone();
                let bc = bc.clone();
                tokio::spawn(async move {
                    db::get_or_create_encryption_key(&pool, &bc, "user", &format!("s{i}"), &keys)
                        .await
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(500)).await;
        // The pool still serves: none of them holds a connection meanwhile.
        let answered: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(answered, 1);
        gate.add_permits(4);
        for provisioning in waiting {
            provisioning.await.unwrap().unwrap();
        }
    });
}

#[test]
fn a_brought_key_is_followed_but_never_rotated_or_retired_by_skilj() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_brought_lifecycle_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let provider = InMemoryKeyProvider::new();
        let keys = KeyService::new(provider.clone());
        let theirs = provider.key("customer").unwrap();
        theirs.create().await.unwrap();
        seal_for(&pool, &bc, "1", &keys).await;
        // A schedule that would rotate every key at every check, and no
        // grace period.
        let (_skilj, _) = builder(
            &url,
            keys.clone(),
            quick(Duration::ZERO).every(Duration::from_millis(100)),
        )
        .build()
        .await
        .unwrap();
        db::assign_encryption_key(&pool, &keys, &bc, Some("customer"), false, None)
            .await
            .unwrap();
        wait_for_state(&pool, "in-memory:customer", "the move to their key", |s| {
            s.rewrapped_at.is_some()
        })
        .await;
        // Their own rotation is followed: recorded and rewrapped...
        theirs.rotate().await.unwrap();
        wait_for_state(
            &pool,
            "in-memory:customer",
            "their rotation followed",
            |s| s.version == 2 && s.rewrapped_at.is_some(),
        )
        .await;
        assert_eq!(wraps(&pool, &bc).await[0].2, Some(2));
        tokio::time::sleep(Duration::from_millis(600)).await;
        // ...but skilj neither rotated it on schedule nor retired version 1.
        let versions = theirs.versions().await.unwrap();
        assert_eq!((versions.latest, versions.min_decryption), (2, 1));
        // The default key, skilj's, was rotated on the schedule meanwhile.
        assert!(keys.provider().versions().await.unwrap().latest > 1);
    });
}

#[test]
fn a_rewrap_is_not_marked_done_over_an_assignment_it_did_not_see() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_epoch_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let keys = KeyService::new(InMemoryKeyProvider::new());
        let (setup, _) = builder(
            &url,
            keys.clone(),
            quick(Duration::ZERO).check_interval(Duration::from_secs(3600)),
        )
        .build()
        .await
        .unwrap();
        setup.shutdown(Duration::from_secs(10)).await;
        db::assign_encryption_key(&pool, &keys, &bc, Some("tenant"), true, None)
            .await
            .unwrap();
        sqlx::query("UPDATE encryption_master_keys SET rewrapped_at = NULL")
            .execute(&pool)
            .await
            .unwrap();

        // A check reads the default key's epoch and the assignments...
        let read = db::master_key_state(&pool, keys.provider_name())
            .await
            .unwrap()
            .unwrap();
        // ...then the context is given the default key back...
        db::assign_encryption_key(&pool, &keys, &bc, None, false, None)
            .await
            .unwrap();
        // ...so that check's rewrap, which didn't cover it, isn't done.
        assert!(!db::mark_data_keys_rewrapped(
            &pool,
            keys.provider_name(),
            read.version,
            read.assignment_epoch
        )
        .await
        .unwrap());
        let now = db::master_key_state(&pool, keys.provider_name())
            .await
            .unwrap()
            .unwrap();
        assert!(now.rewrapped_at.is_none());
        assert!(db::mark_data_keys_rewrapped(
            &pool,
            keys.provider_name(),
            now.version,
            now.assignment_epoch
        )
        .await
        .unwrap());
    });
}

// --- moving data keys to another key, a batch at a time (§246) ---

/// The admin context and the default key's state, with no instance left
/// running: the tests below run the rewrap themselves.
async fn settled(url: &str, keys: &KeyService) {
    let (setup, _) = builder(
        url,
        keys.clone(),
        quick(Duration::ZERO).check_interval(Duration::from_secs(3600)),
    )
    .build()
    .await
    .unwrap();
    setup.shutdown(Duration::from_secs(10)).await;
}

/// `count` active data keys in `bc` wrapped by `provider`, for subjects
/// `<prefix>0000` on, stored in one statement.
async fn seed(pool: &Pool, bc: &str, provider: &dyn KeyProvider, prefix: &str, count: usize) {
    let data_keys: Vec<[u8; 32]> = (0..count)
        .map(|i| {
            let mut key = [7u8; 32];
            key[..8].copy_from_slice(&(i as u64).to_be_bytes());
            key
        })
        .collect();
    let wrapped = provider.wrap_batch(&data_keys).await.unwrap();
    let subjects: Vec<String> = (0..count).map(|i| format!("{prefix}{i:04}")).collect();
    let ciphertexts: Vec<Vec<u8>> = wrapped.iter().map(|w| w.ciphertext.clone()).collect();
    let versions: Vec<i32> = wrapped.iter().map(|w| w.version as i32).collect();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO \"bc_{bc}\".encryption_keys \
         (subject_key, subject_value, status, created_at, wrapped_key, key_provider, \
          master_key_version) \
         SELECT 'user', u.subject, 'active', now(), u.wrapped, $1, u.version \
         FROM UNNEST($2::TEXT[], $3::BYTEA[], $4::INTEGER[]) AS u(subject, wrapped, version)"
    )))
    .bind(provider.name())
    .bind(&subjects)
    .bind(&ciphertexts)
    .bind(&versions)
    .execute(pool)
    .await
    .unwrap();
}

/// `bc` given the key `key`, created, and what a check would then pass to
/// the rewrap: the key, its version and its last rotation.
async fn assign(
    pool: &Pool,
    keys: &KeyService,
    bc: &str,
    key: &str,
) -> (Arc<dyn KeyProvider>, u32, chrono::DateTime<chrono::Utc>) {
    let state = db::assign_encryption_key(pool, keys, bc, Some(key), true, None)
        .await
        .unwrap();
    (
        keys.provider_for(Some(key)).unwrap(),
        state.version,
        state.rotated_at,
    )
}

/// How many active data keys of `bc` each provider wraps, and how many
/// are marked unreadable.
async fn providers_of(pool: &Pool, bc: &str) -> (Vec<(Option<String>, i64)>, i64) {
    let by_provider = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT key_provider, count(*) FROM \"bc_{bc}\".encryption_keys \
         WHERE status = 'active' AND unreadable_since IS NULL \
         GROUP BY key_provider ORDER BY key_provider"
    )))
    .fetch_all(pool)
    .await
    .unwrap();
    let unreadable = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"bc_{bc}\".encryption_keys \
         WHERE status = 'active' AND unreadable_since IS NOT NULL"
    )))
    .fetch_one(pool)
    .await
    .unwrap();
    (by_provider, unreadable)
}

#[test]
fn the_vault_provider_unwraps_and_wraps_a_batch_in_one_request() {
    runtime().block_on(async {
        let (address, fake) = serve_transit().await;
        let vault = VaultTransitProvider::new(&address, "skilj", "test-token");
        let data_keys = [[1u8; 32], [2u8; 32], [3u8; 32]];

        let wrapped = vault.wrap_batch(&data_keys).await.unwrap();
        assert_eq!(wrapped.len(), 3);
        assert!(wrapped.iter().all(|w| w.version == 1));
        assert_eq!(vault.rotate().await.unwrap(), 2);
        let newer = vault.wrap_batch(&data_keys[..1]).await.unwrap();
        assert_eq!(newer[0].version, 2);

        // Versions mixed, and a ciphertext the key refuses among them: one
        // request, one result each, in order.
        let corrupt = WrappedDataKey {
            version: 1,
            ciphertext: b"vault:v1:x.00".to_vec(),
        };
        let batch = [
            wrapped[0].clone(),
            corrupt.clone(),
            newer[0].clone(),
            wrapped[2].clone(),
        ];
        let unwrapped = vault.unwrap_batch(&batch).await.unwrap();
        assert_eq!(unwrapped[0].as_ref().unwrap(), &data_keys[0]);
        assert!(matches!(
            unwrapped[1],
            Err(encryption::Error::DecryptFailed)
        ));
        assert_eq!(unwrapped[2].as_ref().unwrap(), &data_keys[0]);
        assert_eq!(unwrapped[3].as_ref().unwrap(), &data_keys[2]);
        assert_eq!(
            fake.lock().unwrap().requests,
            vec![
                ("encrypt", "skilj".to_string(), 3),
                ("encrypt", "skilj".to_string(), 1),
                ("decrypt", "skilj".to_string(), 4),
            ]
        );

        // Every item refused: Vault answers 400, still read per item; a
        // retired version is told apart from a refused ciphertext.
        vault.retire_below(2).await.unwrap();
        let corrupt = WrappedDataKey {
            version: 2,
            ciphertext: b"vault:v2:x.00".to_vec(),
        };
        let refused = vault
            .unwrap_batch(&[wrapped[0].clone(), corrupt])
            .await
            .unwrap();
        assert!(matches!(
            refused[0],
            Err(encryption::Error::KeyVersionRetired { version: 1 })
        ));
        assert!(matches!(refused[1], Err(encryption::Error::DecryptFailed)));

        // The request failing is no verdict on any data key.
        let wrong = VaultTransitProvider::new(&address, "skilj", "nope");
        assert!(matches!(
            wrong.unwrap_batch(&newer).await,
            Err(encryption::Error::ProviderRefused { .. })
        ));
        assert!(matches!(
            wrong.wrap_batch(&data_keys).await,
            Err(encryption::Error::ProviderRefused { .. })
        ));
        fake.lock().unwrap().fault = Some(("decrypt", 0, 503));
        assert!(matches!(
            vault.unwrap_batch(&newer).await,
            Err(encryption::Error::ProviderUnavailable { .. })
        ));
        fake.lock().unwrap().fault = Some(("encrypt", 0, 429));
        assert!(matches!(
            vault.wrap_batch(&data_keys).await,
            Err(encryption::Error::ProviderUnavailable { .. })
        ));
        // Nothing to do is no request.
        let before = fake.lock().unwrap().requests.len();
        assert!(vault.unwrap_batch(&[]).await.unwrap().is_empty());
        assert!(vault.wrap_batch(&[]).await.unwrap().is_empty());
        assert_eq!(fake.lock().unwrap().requests.len(), before);
    });
}

/// A provider with only the single operations, as one written before the
/// batch ones is: it records its calls and how many ran at once, can wait
/// at a gate before wrapping, and its keys share all of that.
#[derive(Clone)]
struct Probe {
    inner: Arc<dyn KeyProvider>,
    calls: Arc<Mutex<Vec<String>>>,
    running: Arc<std::sync::atomic::AtomicUsize>,
    most_running: Arc<std::sync::atomic::AtomicUsize>,
    delay: Duration,
    gate: Option<Arc<tokio::sync::Semaphore>>,
    /// What it answers for `batch_concurrency`, when not the default.
    at_once: Option<usize>,
}

impl Probe {
    fn new(inner: InMemoryKeyProvider) -> Self {
        Self {
            inner: Arc::new(inner),
            calls: Arc::default(),
            running: Arc::default(),
            most_running: Arc::default(),
            delay: Duration::ZERO,
            gate: None,
            at_once: None,
        }
    }

    fn calls(&self, call: &str) -> usize {
        let calls = self.calls.lock().unwrap();
        calls.iter().filter(|c| c.as_str() == call).count()
    }

    async fn enter(&self, call: String) {
        use std::sync::atomic::Ordering;
        self.calls.lock().unwrap().push(call);
        let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.most_running.fetch_max(running, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.running.fetch_sub(1, Ordering::SeqCst);
    }
}

impl KeyProvider for Probe {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn batch_concurrency(&self) -> usize {
        self.at_once
            .unwrap_or(encryption::provider::SCALAR_BATCH_CONCURRENCY)
    }
    fn wrap<'a>(
        &'a self,
        data_key: &'a [u8; 32],
    ) -> skilj::BoxFuture<'a, Result<WrappedDataKey, encryption::Error>> {
        Box::pin(async move {
            self.enter(format!("wrap {}", self.name())).await;
            if let Some(gate) = &self.gate {
                let _permit = gate.acquire().await.unwrap();
            }
            self.inner.wrap(data_key).await
        })
    }
    fn unwrap<'a>(
        &'a self,
        wrapped: &'a WrappedDataKey,
    ) -> skilj::BoxFuture<'a, Result<[u8; 32], encryption::Error>> {
        Box::pin(async move {
            self.enter(format!("unwrap {}", self.name())).await;
            self.inner.unwrap(wrapped).await
        })
    }
    fn versions(&self) -> skilj::BoxFuture<'_, Result<skilj::KeyVersions, encryption::Error>> {
        self.inner.versions()
    }
    fn rotate(&self) -> skilj::BoxFuture<'_, Result<u32, encryption::Error>> {
        self.inner.rotate()
    }
    fn retire_below(&self, version: u32) -> skilj::BoxFuture<'_, Result<(), encryption::Error>> {
        self.inner.retire_below(version)
    }
    fn key(&self, key: &str) -> Result<Arc<dyn KeyProvider>, encryption::Error> {
        Ok(Arc::new(Self {
            inner: self.inner.key(key)?,
            ..self.clone()
        }))
    }
    fn create(&self) -> skilj::BoxFuture<'_, Result<bool, encryption::Error>> {
        self.inner.create()
    }
    fn delete(&self) -> skilj::BoxFuture<'_, Result<(), encryption::Error>> {
        self.inner.delete()
    }
}

#[test]
fn a_provider_without_batch_operations_is_asked_a_few_at_a_time() {
    runtime().block_on(async {
        use std::sync::atomic::Ordering;
        let probe = Probe {
            delay: Duration::from_millis(20),
            ..Probe::new(InMemoryKeyProvider::new())
        };
        let data_keys: Vec<[u8; 32]> = (0..40u8).map(|i| [i; 32]).collect();
        let mut wrapped = probe.wrap_batch(&data_keys).await.unwrap();
        assert_eq!(probe.calls("wrap in-memory"), 40);
        assert_eq!(
            probe.most_running.load(Ordering::SeqCst),
            encryption::provider::SCALAR_BATCH_CONCURRENCY
        );

        // One it refuses fails neither the ones before it nor after it,
        // and the results keep the order asked in.
        wrapped[5].ciphertext = vec![0, 1, 2, 3];
        probe.most_running.store(0, Ordering::SeqCst);
        let unwrapped = probe.unwrap_batch(&wrapped).await.unwrap();
        assert_eq!(probe.calls("unwrap in-memory"), 40);
        assert_eq!(
            probe.most_running.load(Ordering::SeqCst),
            encryption::provider::SCALAR_BATCH_CONCURRENCY
        );
        for (i, result) in unwrapped.iter().enumerate() {
            match result {
                Ok(data_key) => assert_eq!(data_key, &data_keys[i]),
                Err(err) => {
                    assert_eq!(i, 5, "{err}");
                    assert!(matches!(err, encryption::Error::DecryptFailed));
                }
            }
        }

        // A provider says how many itself: fewer for one behind a rate
        // limit, and nothing at once is still one at a time.
        for (at_once, most) in [(3, 3), (1, 1), (0, 1)] {
            let gentle = Probe {
                delay: Duration::from_millis(5),
                at_once: Some(at_once),
                ..Probe::new(InMemoryKeyProvider::new())
            };
            let wrapped = gentle.wrap_batch(&data_keys[..12]).await.unwrap();
            assert_eq!(gentle.most_running.load(Ordering::SeqCst), most);
            gentle.most_running.store(0, Ordering::SeqCst);
            let unwrapped = gentle.unwrap_batch(&wrapped).await.unwrap();
            assert_eq!(gentle.most_running.load(Ordering::SeqCst), most);
            assert!(unwrapped.iter().all(Result::is_ok));
        }
    });
}

#[test]
fn a_move_from_mixed_keys_asks_each_key_once_per_batch() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_mixed_move_test").await else {
            return;
        };
        let (address, fake) = serve_transit().await;
        let vault = VaultTransitProvider::new(&address, "skilj", "test-token");
        let legacy = EncryptionMasterKey::from_bytes([11u8; 32]);
        let keys = KeyService::new(vault.clone())
            .with_legacy_master_key(legacy.clone())
            .with_cache(Duration::ZERO, 0);
        settled(&url, &keys).await;
        let bc = insert_bounded_context(&pool).await;

        // Two from before key providers, and a third that is corrupt.
        let mut sealed = Vec::new();
        for subject in ["legacy-1", "legacy-2", "legacy-corrupt"] {
            let (data_key, mut wrapped, nonce) = encryption::generate_and_wrap_data_key(&legacy);
            if subject == "legacy-corrupt" {
                wrapped = vec![0, 1];
            } else {
                sealed.push((
                    subject,
                    encryption::encrypt_leaf(&data_key, &format!("{subject}@example.com")),
                ));
            }
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO \"bc_{bc}\".encryption_keys \
                 (subject_key, subject_value, status, created_at, wrapped_key, wrap_nonce) \
                 VALUES ('user', $1, 'active', now(), $2, $3)"
            )))
            .bind(subject)
            .bind(&wrapped)
            .bind(&nonce)
            .execute(&pool)
            .await
            .unwrap();
        }
        // Three under the default key's first version, two under its
        // second, and two under a key the context had in between.
        for subject in ["v1-a", "v1-b", "v1-corrupt"] {
            sealed.push((subject, seal_for(&pool, &bc, subject, &keys).await));
        }
        vault.rotate().await.unwrap();
        for subject in ["v2-a", "v2-b"] {
            sealed.push((subject, seal_for(&pool, &bc, subject, &keys).await));
        }
        assign(&pool, &keys, &bc, "previous").await;
        for subject in ["previous-a", "previous-b"] {
            sealed.push((subject, seal_for(&pool, &bc, subject, &keys).await));
        }
        // One the default key refuses, and one naming a key nothing records.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{bc}\".encryption_keys \
             SET wrapped_key = convert_to('vault:v1:x.00', 'UTF8') \
             WHERE subject_value = 'v1-corrupt'"
        )))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO \"bc_{bc}\".encryption_keys \
             (subject_key, subject_value, status, created_at, wrapped_key, key_provider, \
              master_key_version) \
             VALUES ('user', 'unknown', 'active', now(), '\\x00'::bytea, 'vault-transit:x/y', 1)"
        )))
        .execute(&pool)
        .await
        .unwrap();
        sealed.retain(|(subject, _)| *subject != "v1-corrupt");

        let (target, version, since) = assign(&pool, &keys, &bc, "tenant").await;
        fake.lock().unwrap().requests.clear();
        fake.lock().unwrap().calls.clear();
        let outcome =
            db::rewrap_data_keys(&pool, &bc, &keys, target.as_ref(), version, since, None)
                .await
                .unwrap();
        assert_eq!((outcome.rewrapped, outcome.unreadable), (8, 3));

        // One decrypt per key the rows name - both of the default key's
        // versions in one - and one encrypt by the target, for the batch.
        let mut requests = fake.lock().unwrap().requests.clone();
        requests.sort();
        assert_eq!(
            requests,
            vec![
                ("decrypt", "previous".to_string(), 2),
                ("decrypt", "skilj".to_string(), 5),
                ("encrypt", "tenant".to_string(), 8),
            ]
        );
        assert!(!fake.lock().unwrap().calls.iter().any(|c| c == "rewrap"));
        let (by_provider, unreadable) = providers_of(&pool, &bc).await;
        assert_eq!(
            by_provider,
            vec![(Some("vault-transit:transit/tenant".to_string()), 8)]
        );
        assert_eq!(unreadable, 3);
        // The data keys are the same ones: what they sealed still opens.
        for (subject, ciphertext) in &sealed {
            assert_eq!(
                open_for(&pool, &bc, subject, &keys, ciphertext).await,
                format!("{subject}@example.com")
            );
        }
        let recorded = events_about(&pool, "EncryptionDataKeysRewrapped", &bc).await;
        assert_eq!(recorded.len(), 1, "{recorded:?}");
        assert_eq!(
            (
                recorded[0]["rewrapped"].clone(),
                recorded[0]["unreadable"].clone()
            ),
            (json!(8), json!(3))
        );

        // Nothing is behind any more: no request, no second event.
        fake.lock().unwrap().requests.clear();
        let again = db::rewrap_data_keys(&pool, &bc, &keys, target.as_ref(), version, since, None)
            .await
            .unwrap();
        assert_eq!(again, db::RewrapOutcome::default());
        assert!(fake.lock().unwrap().requests.is_empty());
        assert_eq!(
            events_about(&pool, "EncryptionDataKeysRewrapped", &bc)
                .await
                .len(),
            1
        );
    });
}

#[test]
fn a_failing_provider_mid_move_keeps_the_finished_batches_and_marks_nothing() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_move_outage_test").await else {
            return;
        };
        let (address, fake) = serve_transit().await;
        let vault = VaultTransitProvider::new(&address, "skilj", "test-token");
        let keys = KeyService::new(vault.clone()).with_cache(Duration::ZERO, 0);
        settled(&url, &keys).await;
        let bc = insert_bounded_context(&pool).await;
        // Three batches' worth.
        seed(&pool, &bc, &vault, "s", 450).await;
        let (target, version, since) = assign(&pool, &keys, &bc, "tenant").await;
        let tenant = Some("vault-transit:transit/tenant".to_string());
        let default = Some("vault-transit:transit/skilj".to_string());
        let run = || db::rewrap_data_keys(&pool, &bc, &keys, target.as_ref(), version, since, None);

        // The source key stops answering at the second batch: the first is
        // stored, the run ends with the outage, and no row is marked.
        fake.lock().unwrap().fault = Some(("decrypt", 1, 503));
        let err = run().await.unwrap_err();
        assert_eq!(err.code(), "encryption_key_provider_unavailable", "{err}");
        assert_eq!(
            providers_of(&pool, &bc).await,
            (vec![(default.clone(), 250), (tenant.clone(), 200)], 0)
        );
        // Not done, so not recorded: the run that finishes it records it.
        assert!(events_about(&pool, "EncryptionDataKeysRewrapped", &bc)
            .await
            .is_empty());

        // The next run starts where the rows stand. Its first batch is
        // unwrapped, and then the target's token may not wrap: nothing of
        // that batch is stored, and still no row is marked.
        fake.lock().unwrap().fault = Some(("encrypt", 0, 403));
        let err = run().await.unwrap_err();
        assert_eq!(err.code(), "encryption_key_provider_refused", "{err}");
        assert_eq!(
            providers_of(&pool, &bc).await,
            (vec![(default.clone(), 250), (tenant.clone(), 200)], 0)
        );

        // A token that may not decrypt is no verdict on the data keys.
        fake.lock().unwrap().fault = Some(("decrypt", 0, 403));
        let err = run().await.unwrap_err();
        assert_eq!(err.code(), "encryption_key_provider_refused", "{err}");
        assert_eq!(providers_of(&pool, &bc).await.1, 0);

        // And the one that gets through finishes it, with its own counts.
        fake.lock().unwrap().requests.clear();
        let outcome = run().await.unwrap();
        assert_eq!((outcome.rewrapped, outcome.unreadable), (250, 0));
        assert_eq!(
            fake.lock().unwrap().requests,
            vec![
                ("decrypt", "skilj".to_string(), 200),
                ("encrypt", "tenant".to_string(), 200),
                ("decrypt", "skilj".to_string(), 50),
                ("encrypt", "tenant".to_string(), 50),
            ]
        );
        assert_eq!(providers_of(&pool, &bc).await, (vec![(tenant, 450)], 0));
        let recorded = events_about(&pool, "EncryptionDataKeysRewrapped", &bc).await;
        assert_eq!(recorded.len(), 1, "{recorded:?}");
        assert_eq!(recorded[0]["rewrapped"], 250);
    });
}

#[test]
fn a_subject_erased_while_its_batch_is_at_the_provider_stays_erased() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_move_erasure_test").await else {
            return;
        };
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let probe = Probe {
            gate: Some(Arc::clone(&gate)),
            ..Probe::new(InMemoryKeyProvider::new())
        };
        let keys = KeyService::new(probe.clone());
        // The default key's own wraps don't wait at the gate here.
        gate.add_permits(100);
        settled(&url, &keys).await;
        let bc = insert_bounded_context(&pool).await;
        seed(&pool, &bc, &probe, "s", 12).await;
        let (target, version, since) = assign(&pool, &keys, &bc, "tenant").await;
        gate.forget_permits(usize::MAX);

        let rewrap = {
            let (pool, bc, keys) = (pool.clone(), bc.clone(), keys.clone());
            tokio::spawn(async move {
                db::rewrap_data_keys(&pool, &bc, &keys, target.as_ref(), version, since, None).await
            })
        };
        // Every data key is unwrapped and the target is being asked.
        for _ in 0..400 {
            if probe.calls("wrap in-memory:tenant") > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(probe.calls("unwrap in-memory"), 12);
        assert!(probe.calls("wrap in-memory:tenant") > 0);

        // Meanwhile one subject is forgotten, and another instance's
        // rewrap stores one row.
        db::destroy_encryption_key(&pool, &bc, "user", "s0003", test_now())
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{bc}\".encryption_keys \
             SET wrapped_key = '\\xfeed'::bytea, key_provider = 'in-memory:tenant', \
                 master_key_version = 1 \
             WHERE subject_value = 's0007'"
        )))
        .execute(&pool)
        .await
        .unwrap();
        gate.add_permits(100);
        let outcome = rewrap.await.unwrap().unwrap();

        // Both win: the batch stores only the rows that still hold what it
        // read, and counts only those.
        assert_eq!((outcome.rewrapped, outcome.unreadable), (10, 0));
        let erased: (String, Option<Vec<u8>>) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT status, wrapped_key FROM \"bc_{bc}\".encryption_keys \
             WHERE subject_value = 's0003'"
        )))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(erased, ("destroyed".to_string(), None));
        let stored = wraps(&pool, &bc).await;
        assert_eq!(stored.len(), 11);
        assert!(stored
            .iter()
            .all(|(_, provider, _, _)| provider.as_deref() == Some("in-memory:tenant")));
        let other = stored.iter().find(|(s, ..)| s == "s0007").unwrap();
        assert_eq!(other.3, vec![0xfe, 0xed]);
        let recorded = events_about(&pool, "EncryptionDataKeysRewrapped", &bc).await;
        assert_eq!(recorded.len(), 1, "{recorded:?}");
        assert_eq!(recorded[0]["rewrapped"], 10);
    });
}

#[test]
fn data_keys_a_moved_context_left_behind_are_tried_once_more_and_do_not_hold_up_the_retirement() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_stragglers_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let provider = InMemoryKeyProvider::new();
        let keys = KeyService::new(provider.clone()).with_cache(Duration::ZERO, 0);
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::ZERO))
            .build()
            .await
            .unwrap();
        let mut sealed = Vec::new();
        for subject in ["moved", "corrupt", "put-right", "read-again"] {
            sealed.push((subject, seal_for(&pool, &bc, subject, &keys).await));
        }
        // Three the default key can't unwrap while the context moves to a
        // key of its own: they are marked and stay under the default key.
        let original: Vec<(String, Vec<u8>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT subject_value, wrapped_key FROM \"bc_{bc}\".encryption_keys \
             WHERE subject_value <> 'moved'"
        )))
        .fetch_all(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{bc}\".encryption_keys SET wrapped_key = '\\x00010203'::bytea \
             WHERE subject_value <> 'moved'"
        )))
        .execute(&pool)
        .await
        .unwrap();
        db::assign_encryption_key(&pool, &keys, &bc, Some("tenant"), true, None)
            .await
            .unwrap();
        wait_for_state(&pool, "in-memory:tenant", "the move", |s| {
            s.rewrapped_at.is_some()
        })
        .await;
        assert_eq!(
            providers_of(&pool, &bc).await,
            (vec![(Some("in-memory:tenant".to_string()), 1)], 3)
        );
        // Two of them are put right afterwards - one still marked, one
        // whose mark a read has cleared since.
        for (subject, wrapped) in &original {
            if subject == "corrupt" {
                continue;
            }
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE \"bc_{bc}\".encryption_keys SET wrapped_key = $1, \
                 unreadable_since = CASE WHEN $2 = 'read-again' THEN NULL \
                                         ELSE unreadable_since END \
                 WHERE subject_value = $2"
            )))
            .bind(wrapped)
            .bind(subject)
            .execute(&pool)
            .await
            .unwrap();
        }

        // A leak response on the default key, which no context of its own
        // has anything under: the three count against the retirement
        // again, and only the retirement itself tries them.
        db::rotate_master_key(
            &pool,
            keys.provider(),
            None,
            db::Rotation::Rotate(skilj_core::encryption::trail::RotationReason::Requested),
            true,
            None,
            None,
        )
        .await
        .unwrap();
        wait_for_state(&pool, "in-memory", "the retirement", |s| {
            s.version == 2 && s.retired_below == 2
        })
        .await;

        // What unwraps has moved to the context's key and opens; what
        // doesn't is marked again, after the rotation, and left.
        let rotated_at = db::master_key_state(&pool, "in-memory")
            .await
            .unwrap()
            .unwrap()
            .rotated_at;
        let rows: Vec<(
            String,
            Option<String>,
            Option<chrono::DateTime<chrono::Utc>>,
        )> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT subject_value, key_provider, unreadable_since \
                 FROM \"bc_{bc}\".encryption_keys WHERE status = 'active' ORDER BY subject_value"
        )))
        .fetch_all(&pool)
        .await
        .unwrap();
        for (subject, provider, unreadable_since) in &rows {
            if subject == "corrupt" {
                assert_eq!(provider.as_deref(), Some("in-memory"));
                assert!(unreadable_since.is_some_and(|at| at > rotated_at));
            } else {
                assert_eq!(provider.as_deref(), Some("in-memory:tenant"), "{subject}");
                assert!(unreadable_since.is_none(), "{subject}");
            }
        }
        for (subject, ciphertext) in &sealed {
            if *subject != "corrupt" {
                assert_eq!(
                    open_for(&pool, &bc, subject, &keys, ciphertext).await,
                    format!("{subject}@example.com")
                );
            }
        }
        // The move and the last try each recorded what they did, once.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let recorded = events_about(&pool, "EncryptionDataKeysRewrapped", &bc).await;
        let counts: Vec<(Value, Value)> = recorded
            .iter()
            .map(|e| (e["rewrapped"].clone(), e["unreadable"].clone()))
            .collect();
        assert_eq!(
            counts,
            vec![(json!(1), json!(3)), (json!(2), json!(1))],
            "{recorded:?}"
        );
        // The last try names the key whose retirement asked for it.
        assert_eq!(recorded[0]["retiring_provider"], Value::Null);
        assert_eq!(recorded[1]["retiring_provider"], "in-memory");
        assert_eq!(recorded[1]["provider"], "in-memory:tenant");
        assert_eq!(
            admin_events(&pool, "EncryptionMasterKeyRetired")
                .await
                .len(),
            1
        );
    });
}

#[test]
fn a_data_key_read_again_after_a_move_is_brought_along_and_lets_go_of_the_former_key() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_read_again_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let keys = KeyService::new(InMemoryKeyProvider::new()).with_cache(Duration::ZERO, 0);
        let (_skilj, _) = builder(&url, keys.clone(), quick(Duration::ZERO))
            .build()
            .await
            .unwrap();
        // A key of its own, and under it one data key that key can't
        // unwrap while the context moves on to the next.
        db::assign_encryption_key(&pool, &keys, &bc, Some("former"), true, None)
            .await
            .unwrap();
        wait_for_state(&pool, "in-memory:former", "the first key", |s| {
            s.rewrapped_at.is_some()
        })
        .await;
        let sealed = seal_for(&pool, &bc, "late", &keys).await;
        seal_for(&pool, &bc, "moved", &keys).await;
        let original: Vec<u8> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT wrapped_key FROM \"bc_{bc}\".encryption_keys WHERE subject_value = 'late'"
        )))
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{bc}\".encryption_keys SET wrapped_key = '\\x00010203'::bytea \
             WHERE subject_value = 'late'"
        )))
        .execute(&pool)
        .await
        .unwrap();
        db::assign_encryption_key(&pool, &keys, &bc, Some("current"), true, None)
            .await
            .unwrap();
        wait_for_state(&pool, "in-memory:current", "the move", |s| {
            s.rewrapped_at.is_some()
        })
        .await;
        // Marked, it keeps the former key from being deleted - by design.
        assert_eq!(
            providers_of(&pool, &bc).await,
            (vec![(Some("in-memory:current".to_string()), 1)], 1)
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(admin_events(&pool, "EncryptionKeyDeleted").await.is_empty());

        // Put right, and read: the mark goes, and with it the data key is
        // the current key's business again.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{bc}\".encryption_keys SET wrapped_key = $1 \
             WHERE subject_value = 'late'"
        )))
        .bind(&original)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            open_for(&pool, &bc, "late", &keys, &sealed).await,
            "late@example.com"
        );
        wait_until(&pool, "the data key's move", || async {
            providers_of(&pool, &bc).await == (vec![(Some("in-memory:current".to_string()), 2)], 0)
        })
        .await;
        assert_eq!(
            open_for(&pool, &bc, "late", &keys, &sealed).await,
            "late@example.com"
        );
        // Nothing is wrapped by the former key any more: it is deleted.
        wait_until(&pool, "the former key's deletion", || async {
            !admin_events(&pool, "EncryptionKeyDeleted").await.is_empty()
        })
        .await;
        let deleted = admin_events(&pool, "EncryptionKeyDeleted").await;
        assert_eq!(deleted.len(), 1, "{deleted:?}");
        assert_eq!(deleted[0]["provider"], "in-memory:former");
        // The rewrap is done again, not left pending.
        wait_for_state(&pool, "in-memory:current", "the rewrap done again", |s| {
            s.rewrapped_at.is_some()
        })
        .await;
    });
}

/// The race CI found: a rewrap check that ran while the data key was still
/// marked passed it by, and marked the rewrap done after the read had
/// made it pending - so the data key stayed under the former key.
#[test]
fn a_rewrap_is_not_marked_done_over_a_data_key_read_again_meanwhile() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_read_again_epoch_test").await else {
            return;
        };
        let bc = insert_bounded_context(&pool).await;
        let keys = KeyService::new(InMemoryKeyProvider::new()).with_cache(Duration::ZERO, 0);
        settled(&url, &keys).await;
        db::assign_encryption_key(&pool, &keys, &bc, Some("former"), true, None)
            .await
            .unwrap();
        let sealed = seal_for(&pool, &bc, "late", &keys).await;
        db::assign_encryption_key(&pool, &keys, &bc, Some("current"), true, None)
            .await
            .unwrap();
        // The move marked it unreadable and left it under the former key.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{bc}\".encryption_keys SET unreadable_since = now() \
             WHERE subject_value = 'late'"
        )))
        .execute(&pool)
        .await
        .unwrap();

        // Twice: with the rewrap pending already, and with it done.
        for done_before in [false, true] {
            if done_before {
                let state = db::master_key_state(&pool, "in-memory:current")
                    .await
                    .unwrap()
                    .unwrap();
                assert!(db::mark_data_keys_rewrapped(
                    &pool,
                    "in-memory:current",
                    state.version,
                    state.assignment_epoch
                )
                .await
                .unwrap());
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE \"bc_{bc}\".encryption_keys SET unreadable_since = now() \
                     WHERE subject_value = 'late'"
                )))
                .execute(&pool)
                .await
                .unwrap();
            }
            // A check reads the key's epoch, and finds the data key marked...
            let read = db::master_key_state(&pool, "in-memory:current")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                providers_of(&pool, &bc).await,
                (vec![], 1),
                "done before: {done_before}"
            );
            // ...then the data key is read, and loses its mark...
            assert_eq!(
                open_for(&pool, &bc, "late", &keys, &sealed).await,
                "late@example.com"
            );
            assert_eq!(
                providers_of(&pool, &bc).await,
                (vec![(Some("in-memory:former".to_string()), 1)], 0)
            );
            // ...so that check's rewrap, which passed it by, isn't done.
            assert!(
                !db::mark_data_keys_rewrapped(
                    &pool,
                    "in-memory:current",
                    read.version,
                    read.assignment_epoch
                )
                .await
                .unwrap(),
                "done before: {done_before}"
            );
            let now = db::master_key_state(&pool, "in-memory:current")
                .await
                .unwrap()
                .unwrap();
            assert!(now.rewrapped_at.is_none());
        }
    });
}

/// How [`Asked`] has its key answer a batch.
#[derive(Clone, Copy, PartialEq)]
enum Asking {
    /// One call after the other: what the rewrap did before §246.
    RowByRow,
    /// The trait's defaults: single calls, a few at a time.
    Defaults,
    /// The provider's own batch operations.
    Batches,
}

/// A provider asked for a batch the way `asking` says, its keys too.
struct Asked {
    inner: Arc<dyn KeyProvider>,
    asking: Asking,
}

impl KeyProvider for Asked {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn wrap<'a>(
        &'a self,
        data_key: &'a [u8; 32],
    ) -> skilj::BoxFuture<'a, Result<WrappedDataKey, encryption::Error>> {
        self.inner.wrap(data_key)
    }
    fn unwrap<'a>(
        &'a self,
        wrapped: &'a WrappedDataKey,
    ) -> skilj::BoxFuture<'a, Result<[u8; 32], encryption::Error>> {
        self.inner.unwrap(wrapped)
    }
    fn unwrap_batch<'a>(
        &'a self,
        wrapped: &'a [WrappedDataKey],
    ) -> skilj::BoxFuture<'a, skilj::UnwrappedBatch> {
        Box::pin(async move {
            match self.asking {
                Asking::Batches => self.inner.unwrap_batch(wrapped).await,
                Asking::Defaults => Unbatched(self.inner.as_ref()).unwrap_batch(wrapped).await,
                Asking::RowByRow => {
                    let mut out = Vec::with_capacity(wrapped.len());
                    for one in wrapped {
                        out.push(self.inner.unwrap(one).await);
                    }
                    Ok(out)
                }
            }
        })
    }
    fn wrap_batch<'a>(
        &'a self,
        data_keys: &'a [[u8; 32]],
    ) -> skilj::BoxFuture<'a, Result<Vec<WrappedDataKey>, encryption::Error>> {
        Box::pin(async move {
            match self.asking {
                Asking::Batches => self.inner.wrap_batch(data_keys).await,
                Asking::Defaults => Unbatched(self.inner.as_ref()).wrap_batch(data_keys).await,
                Asking::RowByRow => {
                    let mut out = Vec::with_capacity(data_keys.len());
                    for one in data_keys {
                        out.push(self.inner.wrap(one).await?);
                    }
                    Ok(out)
                }
            }
        })
    }
    fn versions(&self) -> skilj::BoxFuture<'_, Result<skilj::KeyVersions, encryption::Error>> {
        self.inner.versions()
    }
    fn rotate(&self) -> skilj::BoxFuture<'_, Result<u32, encryption::Error>> {
        self.inner.rotate()
    }
    fn retire_below(&self, version: u32) -> skilj::BoxFuture<'_, Result<(), encryption::Error>> {
        self.inner.retire_below(version)
    }
    fn key(&self, key: &str) -> Result<Arc<dyn KeyProvider>, encryption::Error> {
        Ok(Arc::new(Self {
            inner: self.inner.key(key)?,
            asking: self.asking,
        }))
    }
    fn create(&self) -> skilj::BoxFuture<'_, Result<bool, encryption::Error>> {
        self.inner.create()
    }
    fn delete(&self) -> skilj::BoxFuture<'_, Result<(), encryption::Error>> {
        self.inner.delete()
    }
}

/// `0`'s single operations only, so its batches are the trait's defaults.
struct Unbatched<'p>(&'p dyn KeyProvider);

impl Unbatched<'_> {
    async fn unwrap_batch(&self, wrapped: &[WrappedDataKey]) -> skilj::UnwrappedBatch {
        use futures_util::StreamExt;
        let calls: Vec<_> = wrapped.iter().map(|one| self.0.unwrap(one)).collect();
        Ok(futures_util::stream::iter(calls)
            .buffered(encryption::provider::SCALAR_BATCH_CONCURRENCY)
            .collect()
            .await)
    }

    async fn wrap_batch(
        &self,
        data_keys: &[[u8; 32]],
    ) -> Result<Vec<WrappedDataKey>, encryption::Error> {
        use futures_util::{StreamExt, TryStreamExt};
        let calls: Vec<_> = data_keys.iter().map(|one| self.0.wrap(one)).collect();
        futures_util::stream::iter(calls)
            .buffered(encryption::provider::SCALAR_BATCH_CONCURRENCY)
            .try_collect()
            .await
    }
}

/// A line of `/proc/self/status` in KiB, `0` where there is none.
fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with(field))
                .and_then(|line| line.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

/// The measurement behind docs/benchmarks/key-migration-batches (§246):
/// a move of 1 000 data keys - five rewrap batches - from the default
/// Vault key to a context's own, against the imitation of Transit with
/// each request taking a set time, asked row by row (as before), by the
/// trait's defaults, and by Vault's batch operations.
///
/// `cargo test --release -p skilj --test master_key_rotation \
///  move_measurement -- --ignored --nocapture`
#[test]
#[ignore = "a measurement, run by hand"]
fn move_measurement() {
    runtime().block_on(async {
        let Some((url, setup)) = test_db("skilj_master_key_move_measurement").await else {
            return;
        };
        const DATA_KEYS: usize = 1_000;
        let (address, fake) = serve_transit().await;
        let vault = VaultTransitProvider::new(&address, "skilj", "test-token");
        settled(
            &url,
            &KeyService::new(vault.clone()).with_cache(Duration::ZERO, 0),
        )
        .await;
        let pool = skilj_core::db::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .unwrap();
        println!(
            "| Request latency | Asked | Requests | Elapsed | Per data key | Connections in use, most | Longest transaction seen | RSS after | Peak RSS |"
        );
        println!("| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
        for latency in [0u64, 1, 5, 20] {
            for (label, asking) in [
                ("row by row (before)", Asking::RowByRow),
                ("trait defaults", Asking::Defaults),
                ("Vault batches", Asking::Batches),
            ] {
                let keys = KeyService::new(Asked {
                    inner: Arc::new(vault.clone()),
                    asking,
                })
                .with_cache(Duration::ZERO, 0);
                let bc = insert_bounded_context(&setup).await;
                fake.lock().unwrap().delay = Duration::ZERO;
                seed(&setup, &bc, &vault, "s", DATA_KEYS).await;
                let key = format!("m{}", generate_token_id());
                let (target, version, since) = assign(&setup, &keys, &bc, &key[..20]).await;
                fake.lock().unwrap().delay = Duration::from_millis(latency);
                fake.lock().unwrap().requests.clear();

                // Sampled from another pool: the connections the rewrap's
                // pool has out, and the age of its oldest open transaction.
                let most_in_use = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let longest_transaction = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let sampler = {
                    let (pool, setup) = (pool.clone(), setup.clone());
                    let (most, longest) =
                        (Arc::clone(&most_in_use), Arc::clone(&longest_transaction));
                    tokio::spawn(async move {
                        use std::sync::atomic::Ordering;
                        loop {
                            let in_use = pool.size() as usize - pool.num_idle();
                            most.fetch_max(in_use, Ordering::SeqCst);
                            let micros: i64 = sqlx::query_scalar(
                                "SELECT coalesce(max(extract(epoch FROM \
                                     clock_timestamp() - xact_start) * 1000000), 0)::BIGINT \
                                 FROM pg_stat_activity \
                                 WHERE datname = current_database() \
                                 AND pid <> pg_backend_pid() AND xact_start IS NOT NULL",
                            )
                            .fetch_one(&setup)
                            .await
                            .unwrap();
                            longest.fetch_max(micros.max(0) as u64, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_micros(200)).await;
                        }
                    })
                };
                let started = std::time::Instant::now();
                let outcome =
                    db::rewrap_data_keys(&pool, &bc, &keys, target.as_ref(), version, since, None)
                        .await
                        .unwrap();
                let elapsed = started.elapsed();
                sampler.abort();
                assert_eq!(outcome.rewrapped, DATA_KEYS as u64);
                let requests = fake.lock().unwrap().requests.len();
                println!(
                    "| {latency} ms | {label} | {requests} | {:.0} ms | {:.3} ms | {} | {:.1} ms | {} KiB | {} KiB |",
                    elapsed.as_secs_f64() * 1000.0,
                    elapsed.as_secs_f64() * 1000.0 / DATA_KEYS as f64,
                    most_in_use.load(std::sync::atomic::Ordering::SeqCst),
                    longest_transaction.load(std::sync::atomic::Ordering::SeqCst) as f64 / 1000.0,
                    status_kib("VmRSS:"),
                    status_kib("VmHWM:"),
                );
            }
        }
    });
}
