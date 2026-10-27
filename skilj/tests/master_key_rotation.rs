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
    for _ in 0..200 {
        if let Some(state) = db::master_key_state(pool, provider).await.unwrap() {
            if done(&state) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "timed out waiting for {what}: {:?}",
        db::master_key_state(pool, provider).await.unwrap()
    );
}

fn quick(grace: Duration) -> MasterKeyRotation {
    MasterKeyRotation::new()
        .check_interval(Duration::from_millis(50))
        .retire_after(grace)
}

fn builder(url: &str, keys: KeyService, rotation: MasterKeyRotation) -> SkiljBuilder {
    Skilj::builder(url.to_string())
        .key_service(keys)
        .master_key_rotation(rotation)
}

async fn refused(builder: SkiljBuilder) -> String {
    match builder.build().await {
        Ok(_) => panic!("startup was expected to be refused"),
        Err(err) => err.code().to_string(),
    }
}

// --- an imitation of Vault's Transit API ---

#[derive(Default)]
struct Transit {
    /// One entry per version; the "ciphertext" is the plaintext in hex,
    /// since only the version handling is under test here.
    latest: u32,
    min_decryption: u32,
    calls: Vec<String>,
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
    Ok(unhex(payload))
}

fn encrypt(transit: &Transit, plaintext: &[u8]) -> String {
    format!("vault:v{}:{}", transit.latest, hex(plaintext))
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
        latest: 1,
        min_decryption: 1,
        calls: Vec::new(),
    }));
    let app = Router::new()
        .route(
            "/v1/transit/encrypt/{key}",
            post(|State(f): State<Fake>, headers: HeaderMap, Json(body): Json<Value>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let mut t = f.lock().unwrap();
                t.calls.push("encrypt".into());
                let plaintext = unb64(body["plaintext"].as_str().unwrap());
                let ciphertext = encrypt(&t, &plaintext);
                (StatusCode::OK, Json(json!({ "data": { "ciphertext": ciphertext, "key_version": t.latest } })))
            }),
        )
        .route(
            "/v1/transit/decrypt/{key}",
            post(|State(f): State<Fake>, headers: HeaderMap, Json(body): Json<Value>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let mut t = f.lock().unwrap();
                t.calls.push("decrypt".into());
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
                let results: Vec<Value> = body["batch_input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|item| match decrypt(&t, item["ciphertext"].as_str().unwrap()) {
                        Ok(plaintext) => json!({ "ciphertext": encrypt(&t, &plaintext), "key_version": t.latest }),
                        Err(e) => json!({ "error": e }),
                    })
                    .collect();
                (StatusCode::OK, Json(json!({ "data": { "batch_results": results } })))
            }),
        )
        .route(
            "/v1/transit/keys/{key}",
            get(|State(f): State<Fake>, headers: HeaderMap, Path(_key): Path<String>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                let t = f.lock().unwrap();
                (
                    StatusCode::OK,
                    Json(json!({ "data": { "latest_version": t.latest, "min_decryption_version": t.min_decryption } })),
                )
            })
            .post(|State(f): State<Fake>, headers: HeaderMap, Path(key): Path<String>, Json(body): Json<Value>| async move {
                if let Some(denied) = denied(&headers) {
                    return denied;
                }
                f.lock().unwrap().calls.push(format!("create {key} {}", body["type"].as_str().unwrap_or("")));
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
        tenant.create().await.unwrap();
        tenant.delete().await.unwrap();
        let calls = fake.lock().unwrap().calls.clone();
        assert!(
            calls.contains(&"create tenant-a aes256-gcm96".to_string()),
            "{calls:?}"
        );
        assert!(calls.contains(&"delete tenant-a".to_string()), "{calls:?}");
        assert!(vault.key("../escape").is_err(), "key names are checked");

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
    drop(skilj);
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
async fn wait_until<F, Fut>(what: &str, check: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..200 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
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
        wait_until("the tenant key's deletion", || async {
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

        db::assign_encryption_key(&pool, &keys, &bc, Some("customer-key"), false, None)
            .await
            .unwrap();
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
        wait_until("their key let go of", || async {
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
        sqlx::query("UPDATE encryption_master_keys SET rewrapped_at = now() - interval '2 hours'")
            .execute(&pool)
            .await
            .unwrap();
        wait_for_state(&pool, keys.provider_name(), "the retirement", |s| {
            s.retired_below == 2
        })
        .await;
        // It was rewrapped first, so nothing was lost to the retirement.
        assert_eq!(wraps(&pool, &bc).await[0].2, Some(2));
        assert_eq!(
            events_about(&pool, "EncryptionDataKeysRewrapped", &bc)
                .await
                .len(),
            2
        );
    });
}
