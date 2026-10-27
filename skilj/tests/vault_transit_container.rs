//! The Vault Transit provider against real Vault and real OpenBao
//! (docs/architecture.md §230), in containers: what the in-process
//! imitation in `master_key_rotation.rs` can only assume - the HTTP status
//! of each kind of refusal, a batch rewrap with a failed item,
//! `min_decryption_version`, creating and deleting keys, and a token's
//! permissions - is checked against the servers themselves, and then the
//! whole lifecycle runs end to end on them.
//!
//! Needs Docker, as the broker crates' tests do: skipped with a `skipping:`
//! line when it can't start a container, which fails CI (docs/architecture.md
//! §223).

use serde_json::{json, Value};
use skilj::{
    KeyProvider, KeyService, MasterKeyRotation, Skilj, VaultTransitProvider, WrappedDataKey,
};
use skilj_core::bootstrap::{ContextCreator, ADMIN_BOUNDED_CONTEXT_NAME};
use skilj_core::db::{self, Pool};
use skilj_core::encryption::{self, trail::RotationReason};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::generate_token_id;
use std::time::Duration;
use testcontainers_modules::hashicorp_vault::HashicorpVault;
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const ROOT_TOKEN: &str = "myroot";

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for vault_transit_container tests")
    })
}

/// A running server: Vault's or OpenBao's dev mode, root token
/// [`ROOT_TOKEN`], with Transit enabled and a key `skilj`. The container
/// lives as long as the value.
struct Server {
    address: String,
    _container: Box<dyn std::any::Any + Send>,
}

async fn finish<I: testcontainers_modules::testcontainers::Image + 'static>(
    label: &str,
    container: ContainerAsync<I>,
) -> Option<Server> {
    let host = match container.get_host().await {
        Ok(host) => host,
        Err(e) => {
            eprintln!("skipping: getting the {label} container's host failed: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(8200).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("skipping: getting the {label} container's port failed: {e}");
            return None;
        }
    };
    let address = format!("http://{host}:{port}");
    admin(&address, "sys/mounts/transit", json!({ "type": "transit" })).await;
    admin(
        &address,
        "transit/keys/skilj",
        json!({ "type": "aes256-gcm96" }),
    )
    .await;
    Some(Server {
        address,
        _container: Box::new(container),
    })
}

async fn vault() -> Option<Server> {
    let image = HashicorpVault::default();
    match image.start().await {
        Ok(container) => finish("Vault", container).await,
        Err(e) => {
            eprintln!("skipping: starting the Vault container failed (is Docker running?): {e}");
            None
        }
    }
}

async fn openbao() -> Option<Server> {
    let image = GenericImage::new("openbao/openbao", "2.1.0")
        .with_exposed_port(8200.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/v1/sys/health")
                .with_port(ContainerPort::Tcp(8200))
                .with_expected_status_code(200u16),
        ))
        .with_env_var("BAO_DEV_ROOT_TOKEN_ID", ROOT_TOKEN)
        .with_env_var("BAO_DEV_LISTEN_ADDRESS", "0.0.0.0:8200");
    match image.start().await {
        Ok(container) => finish("OpenBao", container).await,
        Err(e) => {
            eprintln!("skipping: starting the OpenBao container failed (is Docker running?): {e}");
            None
        }
    }
}

/// A root-token write, for setting a server up.
async fn admin(address: &str, path: &str, body: Value) -> Value {
    let response = reqwest::Client::new()
        .post(format!("{address}/v1/{path}"))
        .header("X-Vault-Token", ROOT_TOKEN)
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{path}: {e}"));
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{path}: {status} {text}");
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

/// A token whose policy allows `capabilities` on `path` only.
async fn token_with(address: &str, path: &str, capabilities: &[&str]) -> String {
    let name = format!("p{}", generate_token_id());
    let policy = format!("path \"{path}\" {{ capabilities = {capabilities:?} }}");
    admin(
        address,
        &format!("sys/policies/acl/{name}"),
        json!({ "policy": policy }),
    )
    .await;
    let created = admin(address, "auth/token/create", json!({ "policies": [name] })).await;
    created["auth"]["client_token"]
        .as_str()
        .unwrap()
        .to_string()
}

/// What the provider does against one server: every assumption the
/// imitation makes.
async fn check_provider(server: &Server) {
    let vault = VaultTransitProvider::new(&server.address, "skilj", ROOT_TOKEN);
    let data_key = [42u8; 32];

    // Wrap and unwrap; the version is read from the ciphertext.
    let v1 = vault.wrap(&data_key).await.unwrap();
    assert_eq!(v1.version, 1);
    assert_eq!(vault.unwrap(&v1).await.unwrap(), data_key);

    // Rotation, and a batch rewrap with one bad item: the others are
    // rewrapped and the bad one is the key's refusal, not a failed batch.
    assert_eq!(vault.rotate().await.unwrap(), 2);
    let bad = WrappedDataKey {
        version: 1,
        ciphertext: b"vault:v1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_vec(),
    };
    let results = vault
        .rewrap(&[v1.clone(), bad.clone(), v1.clone()])
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap().version, 2);
    assert!(
        matches!(results[1], Err(encryption::Error::DecryptFailed)),
        "{:?}",
        results[1].as_ref().err()
    );
    assert_eq!(results[2].as_ref().unwrap().version, 2);
    // Every item failing: still per item.
    let all_bad = vault.rewrap(&[bad.clone(), bad.clone()]).await.unwrap();
    assert!(all_bad
        .iter()
        .all(|r| matches!(r, Err(encryption::Error::DecryptFailed))));
    let v2 = results[0].as_ref().unwrap().clone();
    assert_eq!(vault.unwrap(&v2).await.unwrap(), data_key);

    // Retiring: the server itself refuses the old version, and says so.
    vault.retire_below(2).await.unwrap();
    assert!(matches!(
        vault.unwrap(&v1).await,
        Err(encryption::Error::KeyVersionRetired { version: 1 })
    ));
    let versions = vault.versions().await.unwrap();
    assert_eq!((versions.latest, versions.min_decryption), (2, 2));

    // Another key's material: refused as the key's verdict on it.
    let other = vault.key("other-material").unwrap();
    assert!(other.create().await.unwrap(), "created");
    assert!(!other.create().await.unwrap(), "exists: not created again");
    assert!(
        !vault.create().await.unwrap(),
        "an existing key is never claimed"
    );
    assert!(matches!(
        other.unwrap(&v2).await,
        Err(encryption::Error::DecryptFailed)
    ));
    // A key that doesn't exist: refused the same way.
    let missing = vault.key("never-created").unwrap();
    assert!(matches!(
        missing.unwrap(&v2).await,
        Err(encryption::Error::DecryptFailed)
    ));
    other.delete().await.unwrap();
    assert!(other.versions().await.is_err(), "deleted");

    // A token without the right is no verdict on the data key.
    let no_decrypt = token_with(&server.address, "transit/keys/skilj", &["read"]).await;
    let limited = VaultTransitProvider::new(&server.address, "skilj", no_decrypt.clone());
    assert!(matches!(
        limited.unwrap(&v2).await,
        Err(encryption::Error::ProviderRefused { .. })
    ));
    // One that may create but not read can't claim an existing key.
    let create_only = token_with(&server.address, "transit/keys/*", &["create", "update"]).await;
    let blind = VaultTransitProvider::new(&server.address, "skilj", create_only);
    assert!(blind.create().await.is_err(), "refused, not claimed");

    // Nothing listening: unavailable.
    let gone = VaultTransitProvider::new("http://127.0.0.1:1", "skilj", ROOT_TOKEN);
    assert!(matches!(
        gone.versions().await,
        Err(encryption::Error::ProviderUnavailable { .. })
    ));
}

#[test]
fn the_provider_against_vault() {
    runtime().block_on(async {
        let Some(server) = vault().await else {
            return;
        };
        check_provider(&server).await;
    });
}

#[test]
fn the_provider_against_openbao() {
    runtime().block_on(async {
        let Some(server) = openbao().await else {
            return;
        };
        check_provider(&server).await;
    });
}

// --- the lifecycle, end to end ---

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

async fn insert_bounded_context(pool: &Pool) -> String {
    let bc = BoundedContext {
        name: format!("customers_{}", generate_token_id()),
        status: BoundedContextStatus::Active,
        created_at: chrono::Utc::now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    bc.name
}

async fn wait_for(
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
        "timed out waiting for {what}: {:?}",
        db::master_key_state(pool, provider).await.unwrap()
    );
}

async fn admin_events(pool: &Pool, name: &str) -> Vec<Value> {
    db::list_events_for_bounded_context(pool, ADMIN_BOUNDED_CONTEXT_NAME)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.event_type.name == name)
        .map(|e| serde_json::from_str(&e.payload).unwrap())
        .collect()
}

async fn lifecycle(server: &Server, database: &str) {
    let Some((url, pool)) = test_db(database).await else {
        return;
    };
    let provider = VaultTransitProvider::new(&server.address, "skilj", ROOT_TOKEN);
    let keys = KeyService::new(provider.clone()).with_cache(Duration::ZERO, 0);
    let name = provider.name().to_string();
    let bc = insert_bounded_context(&pool).await;
    let tenant = insert_bounded_context(&pool).await;
    let (_, _, data_key) = db::get_or_create_encryption_key(&pool, &bc, "user", "1", &keys)
        .await
        .unwrap();
    let sealed = encryption::encrypt_leaf(&data_key, "1@example.com");
    db::get_or_create_encryption_key(&pool, &tenant, "user", "2", &keys)
        .await
        .unwrap();

    let (_skilj, _) = Skilj::builder(url.clone())
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
        .key_service(keys.clone())
        .master_key_rotation(
            MasterKeyRotation::new()
                .check_interval(Duration::from_millis(100))
                .retire_after(Duration::from_secs(3600)),
        )
        .build()
        .await
        .unwrap();

    // A context of its own key, created in Vault by skilj.
    let assigned = db::assign_encryption_key(&pool, &keys, &tenant, Some("tenant-key"), true, None)
        .await
        .unwrap();
    assert!(assigned.created_by_skilj);
    let tenant_name = assigned.provider.clone();
    wait_for(&pool, &tenant_name, "the tenant's move", |s| {
        s.rewrapped_at.is_some()
    })
    .await;

    // A leak response on the default key: rotated, rewrapped in Vault,
    // and the old version refused by Vault itself.
    db::rotate_master_key(
        &pool,
        keys.provider(),
        None,
        db::Rotation::Rotate(RotationReason::Requested),
        true,
        None,
        None,
    )
    .await
    .unwrap();
    wait_for(&pool, &name, "the retirement", |s| {
        s.version == 2 && s.retired_below == 2
    })
    .await;
    let versions = provider.versions().await.unwrap();
    assert_eq!(versions.min_decryption, 2, "Vault itself refuses version 1");
    let (_, _, again) = db::get_or_create_encryption_key(&pool, &bc, "user", "1", &keys)
        .await
        .unwrap();
    assert_eq!(
        encryption::decrypt_leaf(&again, &sealed).unwrap(),
        "1@example.com"
    );
    assert_eq!(
        admin_events(&pool, "EncryptionMasterKeyRetired")
            .await
            .len(),
        1
    );

    // The tenant's context deleted: after the grace period - shortened
    // here - its key is deleted in Vault.
    db::hard_delete_bounded_context(&pool, &tenant)
        .await
        .unwrap();
    for _ in 0..200 {
        if let Some(state) = db::master_key_state(&pool, &tenant_name).await.unwrap() {
            if state.delete_after.is_some() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sqlx::query("UPDATE encryption_master_keys SET delete_after = now() WHERE provider = $1")
        .bind(&tenant_name)
        .execute(&pool)
        .await
        .unwrap();
    for _ in 0..200 {
        if !admin_events(&pool, "EncryptionKeyDeleted").await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(admin_events(&pool, "EncryptionKeyDeleted").await.len(), 1);
    assert!(
        provider
            .key("tenant-key")
            .unwrap()
            .versions()
            .await
            .is_err(),
        "deleted in Vault"
    );
}

#[test]
fn the_lifecycle_against_vault() {
    runtime().block_on(async {
        let Some(server) = vault().await else {
            return;
        };
        lifecycle(&server, "skilj_vault_lifecycle_test").await;
    });
}

#[test]
fn the_lifecycle_against_openbao() {
    runtime().block_on(async {
        let Some(server) = openbao().await else {
            return;
        };
        lifecycle(&server, "skilj_openbao_lifecycle_test").await;
    });
}
