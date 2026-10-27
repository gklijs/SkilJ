//! Rotating the encryption master key (docs/architecture.md §230): every
//! data key is stored with the fingerprint of the master key it is wrapped
//! under; startup records which key is in use and refuses a process that
//! can't work with it; a process with a reconciliation role switches to
//! its current key and rewraps every data key under it, and both steps are
//! events in the admin bounded context.
//!
//! Each test gets a database of its own: the key in use is one row for the
//! whole database.

use chrono::SubsecRound;
use serde_json::Value;
use skilj::{EncryptionMasterKey, Skilj, SkiljBuilder};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::{ContextCreator, ADMIN_BOUNDED_CONTEXT_NAME};
use skilj_core::db::{self, Pool};
use skilj_core::encryption::{self, DataKey};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::generate_token_id;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for master_key_rotation tests")
    })
}

/// A database of the test's own, or `None` (with a stderr note) when none
/// can be reached.
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

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now().trunc_subsecs(6)
}

fn old_key() -> EncryptionMasterKey {
    EncryptionMasterKey::from_bytes([11u8; 32])
}

fn new_key() -> EncryptionMasterKey {
    EncryptionMasterKey::from_bytes([12u8; 32])
}

async fn insert_role(pool: &Pool) -> Role {
    let role = Role {
        id: generate_token_id(),
        external_subject: unique_name("subject"),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();
    role
}

async fn insert_bounded_context(pool: &Pool) -> BoundedContext {
    let bc = BoundedContext {
        name: unique_name("customers"),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    bc
}

async fn grant_admin(pool: &Pool, role: &Role, bc: &BoundedContext) {
    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        scope: None,
        capabilities: Default::default(),
        delegable: Default::default(),
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(pool, &mapping)
        .await
        .unwrap();
}

/// A data key for `subject` in `bc`, provisioned (or found) the way a
/// sensitive payload's write does, and a value sealed under it.
async fn seal_for(
    pool: &Pool,
    bc: &str,
    subject: &str,
    master_key: &EncryptionMasterKey,
) -> (DataKey, String) {
    let (_, _, data_key) = db::get_or_create_encryption_key(pool, bc, "user", subject, master_key)
        .await
        .unwrap();
    let ciphertext = encryption::encrypt_leaf(&data_key, &format!("{subject}@example.com"));
    (data_key, ciphertext)
}

/// `ciphertext` opened with `subject`'s data key as `master_key` unwraps it.
async fn open_for(
    pool: &Pool,
    bc: &str,
    subject: &str,
    master_key: &EncryptionMasterKey,
    ciphertext: &str,
) -> String {
    let (_, _, data_key) = db::get_or_create_encryption_key(pool, bc, "user", subject, master_key)
        .await
        .unwrap();
    encryption::decrypt_leaf(&data_key, ciphertext).unwrap()
}

/// `(subject_value, master_key_fingerprint)` of every active data key.
async fn wraps(pool: &Pool, bc: &str) -> Vec<(String, Option<String>)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT subject_value, master_key_fingerprint FROM \"bc_{bc}\".encryption_keys \
         WHERE status = 'active' ORDER BY subject_value"
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

fn builder(url: &str, keys: (EncryptionMasterKey, Vec<EncryptionMasterKey>)) -> SkiljBuilder {
    Skilj::builder(url.to_string())
        .encryption_master_key(keys.0)
        .previous_encryption_master_keys(keys.1)
}

async fn refused(builder: SkiljBuilder) -> String {
    match builder.build().await {
        Ok(_) => panic!("startup was expected to be refused"),
        Err(err) => err.code().to_string(),
    }
}

#[test]
fn rotating_the_master_key_rewraps_every_data_key_and_records_when() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_rotation_test").await else {
            return;
        };
        let role = insert_role(&pool).await;
        let administered = insert_bounded_context(&pool).await;
        grant_admin(&pool, &role, &administered).await;
        let other = insert_bounded_context(&pool).await;

        // The first start with a master key records it as the one in use.
        let (first, _) = builder(&url, (old_key(), vec![]))
            .reconciliation_role(role.external_subject.clone())
            .build()
            .await
            .unwrap();
        drop(first);
        assert_eq!(
            db::encryption_master_key_in_use(&pool)
                .await
                .unwrap()
                .as_deref(),
            Some(old_key().fingerprint())
        );
        let activated = admin_events(&pool, "EncryptionMasterKeyActivated").await;
        assert_eq!(activated.len(), 1);
        assert_eq!(activated[0]["fingerprint"], old_key().fingerprint());
        assert_eq!(activated[0]["previous_fingerprint"], Value::Null);

        let (_, sealed_1) = seal_for(&pool, &administered.name, "1", &old_key()).await;
        let (_, sealed_2) = seal_for(&pool, &other.name, "2", &old_key()).await;
        let (_, _) = seal_for(&pool, &administered.name, "forgotten", &old_key()).await;
        db::destroy_encryption_key(&pool, &administered.name, "user", "forgotten", test_now())
            .await
            .unwrap();

        // Rolled out with both keys, without a reconciliation role: the old
        // key stays in use, so new data keys are still wrapped under it.
        let rolling = || (new_key(), vec![old_key()]);
        let (rolled, report) = builder(&url, rolling()).build().await.unwrap();
        assert!(report.skipped_no_access.is_empty());
        let both = new_key().with_previous([old_key()]);
        seal_for(&pool, &administered.name, "3", &both).await;
        assert_eq!(
            wraps(&pool, &administered.name).await,
            vec![
                ("1".to_string(), Some(old_key().fingerprint().to_string())),
                ("3".to_string(), Some(old_key().fingerprint().to_string())),
            ]
        );
        drop(rolled);

        // Only the new key before the rotation: refused, the key in use is
        // none of its own.
        assert_eq!(
            refused(builder(&url, (new_key(), vec![]))).await,
            "encryption_master_key_changed"
        );

        // The rotation: the new key is in use, and every data key of the
        // bounded context the Role administers is rewrapped under it.
        let (rotated, report) = builder(&url, rolling())
            .reconciliation_role(role.external_subject.clone())
            .build()
            .await
            .unwrap();
        drop(rotated);
        assert_eq!(
            report.skipped_no_access,
            vec![format!("{} (encryption master key rotation)", other.name)]
        );
        assert_eq!(
            db::encryption_master_key_in_use(&pool)
                .await
                .unwrap()
                .as_deref(),
            Some(new_key().fingerprint())
        );
        let activated = admin_events(&pool, "EncryptionMasterKeyActivated").await;
        assert_eq!(activated.len(), 2);
        assert_eq!(activated[1]["fingerprint"], new_key().fingerprint());
        assert_eq!(
            activated[1]["previous_fingerprint"],
            old_key().fingerprint()
        );
        assert_eq!(activated[1]["actor_role_id"], role.id.as_str());
        let rewrapped = admin_events(&pool, "EncryptionDataKeysRewrapped").await;
        assert_eq!(rewrapped.len(), 1);
        assert_eq!(rewrapped[0]["bounded_context"], administered.name.as_str());
        assert_eq!(rewrapped[0]["fingerprint"], new_key().fingerprint());
        assert_eq!(rewrapped[0]["rewrapped"], 2);
        assert_eq!(rewrapped[0]["unreadable"], 0);
        assert!(wraps(&pool, &administered.name)
            .await
            .iter()
            .all(|(_, fingerprint)| fingerprint.as_deref() == Some(new_key().fingerprint())));
        // Destroyed stays destroyed: nothing to rewrap, nothing revived.
        assert!(
            db::get_active_encryption_key(&pool, &administered.name, "user", "forgotten")
                .await
                .unwrap()
                .is_none()
        );

        // The data keys themselves didn't change: what was sealed before
        // opens with the new key alone.
        assert_eq!(
            open_for(&pool, &administered.name, "1", &new_key(), &sealed_1).await,
            "1@example.com"
        );

        // The other bounded context's keys are still under the old one, so
        // dropping it is refused, and an instance with only the old key is
        // refused too.
        assert_eq!(
            refused(builder(&url, (new_key(), vec![]))).await,
            "encryption_master_key_unknown"
        );
        assert_eq!(
            refused(builder(&url, (old_key(), vec![]))).await,
            "encryption_master_key_changed"
        );

        // Once a Role administering it has run, the old key can go.
        grant_admin(&pool, &role, &other).await;
        let (finished, report) = builder(&url, rolling())
            .reconciliation_role(role.external_subject.clone())
            .build()
            .await
            .unwrap();
        drop(finished);
        assert!(report.skipped_no_access.is_empty());
        assert_eq!(
            admin_events(&pool, "EncryptionMasterKeyActivated")
                .await
                .len(),
            2,
            "the key in use didn't change again"
        );
        let (_alone, _) = builder(&url, (new_key(), vec![])).build().await.unwrap();
        assert_eq!(
            open_for(&pool, &other.name, "2", &new_key(), &sealed_2).await,
            "2@example.com"
        );
    });
}

#[test]
fn data_keys_from_before_fingerprints_are_recognised_and_stamped() {
    runtime().block_on(async {
        let Some((url, pool)) = test_db("skilj_master_key_unrecorded_test").await else {
            return;
        };
        let role = insert_role(&pool).await;
        let bc = insert_bounded_context(&pool).await;
        grant_admin(&pool, &role, &bc).await;

        // Data keys wrapped by a version that recorded no fingerprint.
        let (_, sealed) = seal_for(&pool, &bc.name, "1", &old_key()).await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{}\".encryption_keys SET master_key_fingerprint = NULL",
            bc.name
        )))
        .execute(&pool)
        .await
        .unwrap();

        // A different key unwraps none of them: refused, and nothing is
        // recorded as in use.
        assert_eq!(
            refused(builder(&url, (new_key(), vec![]))).await,
            "encryption_master_key_changed"
        );
        assert_eq!(db::encryption_master_key_in_use(&pool).await.unwrap(), None);

        // Started with both keys, the key in use is the one that wrapped
        // them, and the reconciliation role rotates to the new one.
        let (_skilj, _) = builder(&url, (new_key(), vec![old_key()]))
            .reconciliation_role(role.external_subject.clone())
            .build()
            .await
            .unwrap();
        let activated = admin_events(&pool, "EncryptionMasterKeyActivated").await;
        assert_eq!(activated.len(), 2);
        assert_eq!(activated[0]["fingerprint"], old_key().fingerprint());
        assert_eq!(activated[0]["actor_role_id"], Value::Null);
        assert_eq!(activated[1]["fingerprint"], new_key().fingerprint());
        assert_eq!(
            wraps(&pool, &bc.name).await,
            vec![("1".to_string(), Some(new_key().fingerprint().to_string()))]
        );
        assert_eq!(
            open_for(&pool, &bc.name, "1", &new_key(), &sealed).await,
            "1@example.com"
        );
    });
}

#[test]
fn previous_keys_need_a_current_one() {
    runtime().block_on(async {
        let builder = Skilj::builder("postgres://unused".to_string())
            .previous_encryption_master_keys([old_key()]);
        let err = match builder.build().await {
            Ok(_) => panic!("previous keys without a current one were accepted"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("previous_encryption_master_keys is configured without"));
    });
}
