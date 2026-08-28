//! End-to-end tests for `skilj_inspector::data` - the read-only layer
//! this crate's whole UI is built on. Same real-Postgres provisioning
//! pattern (`DATABASE_URL`, else embedded, else skip) every other
//! `tests/*.rs` in this project already uses - duplicated here rather
//! than shared, the same call every prior pass already made.
//!
//! Seeds directly through `skilj_core` - never through GraphQL/REST or
//! the `skilj` facade, since this crate depends on `skilj_core` alone
//! and its whole point is reading what's already in Postgres regardless
//! of whether any server surface ever ran against it.

use chrono::SubsecRound;
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::encryption::EncryptionMasterKey;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, EventBroadcaster};
use skilj_core::plugin::ProjectionDispatcher;
use skilj_core::shared::{generate_token_id, generate_token_secret, SensitiveField};
use skilj_inspector::data;

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for skilj-inspector data tests")
    })
}

async fn test_database_url() -> Option<String> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.database_url.clone())
}

async fn check_reachable(database_url: &str, label: &str) -> bool {
    match skilj_core::db::connect(database_url).await {
        Ok(pool) => {
            skilj_core::db::migrate(&pool).await.is_ok() || {
                eprintln!("skipping: migrating {label} failed");
                false
            }
        }
        Err(e) => {
            eprintln!("skipping: connecting to {label} failed: {e}");
            false
        }
    }
}

async fn provision() -> Option<TestDb> {
    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        return check_reachable(&database_url, "DATABASE_URL")
            .await
            .then_some(TestDb {
                database_url,
                _embedded: None,
            });
    }

    let mut server = postgresql_embedded::PostgreSQL::default();
    if let Err(e) = server.setup().await {
        eprintln!(
            "skipping: DATABASE_URL not set and embedded PostgreSQL setup failed \
             (no network egress to fetch the binary, or a missing system library \
             like libxml2 it links against): {e}"
        );
        return None;
    }
    if let Err(e) = server.start().await {
        eprintln!("skipping: embedded PostgreSQL failed to start: {e}");
        return None;
    }
    let database_name = "skilj_inspector_test";
    if let Err(e) = server.create_database(database_name).await {
        eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
        return None;
    }
    let url = server.settings().url(database_name);
    if !check_reachable(&url, "embedded PostgreSQL").await {
        return None;
    }
    Some(TestDb {
        database_url: url,
        _embedded: Some(server),
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now().trunc_subsecs(6)
}

/// Nothing registered anywhere - `keys`/`project`/`default_state` all
/// return `None`, the "pair isn't registered at all" convention every
/// one of them shares. Seeding one event here needs no projection at
/// all, so this is the correct dispatcher, not a shortcut.
struct NoopProjectionDispatcher;

impl ProjectionDispatcher for NoopProjectionDispatcher {
    fn keys(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
        _event: &skilj_core::event_store::Event,
    ) -> Option<Vec<String>> {
        None
    }

    fn project(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
        _state_json: &str,
        _event: &skilj_core::event_store::Event,
        _key: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }

    fn default_state(&self, _bounded_context: &str, _projection_name: &str) -> Option<String> {
        None
    }
}

#[test]
fn loads_registered_types_projections_and_events_for_a_bounded_context() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let pool = skilj_core::db::connect(&database_url).await.unwrap();

        let admin_role = Role {
            id: generate_token_id(),
            external_subject: unique_name("admin"),
            name: "Admin".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };

        let bc_name = unique_name("banking");
        let bc = BoundedContext {
            name: bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
        };
        skilj_core::db::insert_bounded_context(&pool, &bc)
            .await
            .unwrap();

        let admin_mapping = RoleAccessMapping {
            role: admin_role,
            bounded_context: bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };

        // One EventType with a sensitive field ("email"), naming
        // user_id as its subject - the exact shape `decrypt_on_read.rs`
        // (skilj/tests/) already establishes for a real ciphertext
        // round trip.
        let event_type_registration = skilj_core::event_store::register_event_type(
            &admin_mapping,
            &bc,
            "AccountOpened".to_string(),
            serde_json::json!({
                "type": "object",
                "properties": {
                    "email": {"type": "string"},
                    "user_id": {"type": "string"}
                }
            })
            .to_string(),
            vec![],
            vec![SensitiveField {
                field: "email".to_string(),
                subject_key: "user".to_string(),
                subject_field: "user_id".to_string(),
            }],
            false,
            true,
            false,
            None,
            None,
            true,
            None,
            test_now(),
        )
        .unwrap();
        let event_type = event_type_registration.event_type().clone();
        skilj_core::db::upsert_event_type(&pool, &event_type)
            .await
            .unwrap();

        let command_type_registration = skilj_core::event_store::register_command_type(
            &admin_mapping,
            &bc,
            "WithdrawMoney".to_string(),
            "{}".to_string(),
            vec![],
            vec![],
            true,
            None,
        )
        .unwrap();
        skilj_core::db::upsert_command_type(&pool, command_type_registration.command_type())
            .await
            .unwrap();

        let projection_registration = skilj_core::projections::register_projection(
            &admin_mapping,
            &bc,
            "AccountBalance".to_string(),
            serde_json::json!({"type": "object", "properties": {"balance": {"type": "integer"}}})
                .to_string(),
            vec![event_type.clone()],
            false,
            None,
            None,
            &[],
        )
        .unwrap();
        if let skilj_core::projections::ProjectionRegistration::Created { projection, .. } =
            &projection_registration
        {
            skilj_core::db::upsert_projection(&pool, projection)
                .await
                .unwrap();
        } else {
            panic!("first registration of a new projection must be Created");
        }

        // One real event, with a real plaintext value for the sensitive
        // field - `create_and_insert_direct_event` is the exact function
        // the REST direct-creation endpoint calls, so this is a genuine
        // encrypted write, not a hand-rolled approximation of one.
        let token = skilj_core::access_control::create_direct_creation_token(
            &admin_mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            test_now(),
        )
        .unwrap();
        skilj_core::db::insert_direct_creation_token(&pool, &token)
            .await
            .unwrap();

        let master_key = EncryptionMasterKey::from_bytes([7u8; 32]);
        let plaintext_email = "definitely-plaintext@example.test";
        skilj_core::db::create_and_insert_direct_event(
            &pool,
            &NoopProjectionDispatcher,
            &EventBroadcaster::new(16),
            &EventCache::new(16),
            &token,
            serde_json::json!({"email": plaintext_email, "user_id": "u1"}).to_string(),
            test_now(),
            Some(&master_key),
        )
        .await
        .unwrap();

        // --- the actual assertions: skilj-inspector's own read layer ---

        let bounded_contexts = data::load_bounded_contexts(&pool).await.unwrap();
        assert!(bounded_contexts.iter().any(|b| b.name == bc_name));

        let loaded = data::load_bounded_context_data(&pool, &bc_name)
            .await
            .unwrap();

        assert_eq!(loaded.event_types.len(), 1);
        assert_eq!(loaded.event_types[0].name, "AccountOpened");

        assert_eq!(loaded.command_types.len(), 1);
        assert_eq!(loaded.command_types[0].name, "WithdrawMoney");

        assert_eq!(loaded.projections.len(), 1);
        assert_eq!(loaded.projections[0].name, "AccountBalance");

        assert_eq!(loaded.recent_events.len(), 1);
        let event = &loaded.recent_events[0];
        assert_eq!(event.event_type.name, "AccountOpened");
        // The concrete proof the "ciphertext only, ever" decision (see
        // this crate's own root doc comment) needs no special-case code:
        // the payload this read layer hands back is exactly the stored
        // row, and the stored row already never contained the plaintext
        // - `encrypt_leaf` substituted ciphertext at write time, before
        // this crate's read path ever runs.
        assert!(
            !event.payload.contains(plaintext_email),
            "a sensitive field's plaintext must never appear in what this read-only tool \
             renders - got payload: {}",
            event.payload
        );
        assert!(
            event.payload.contains("u1"),
            "the non-sensitive field must still read back in the clear: {}",
            event.payload
        );
    });
}

#[test]
fn returns_empty_lists_for_an_unregistered_bounded_context() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let pool = skilj_core::db::connect(&database_url).await.unwrap();

        let loaded = data::load_bounded_context_data(&pool, "nonexistent_context_xyz")
            .await
            .unwrap();
        assert!(loaded.event_types.is_empty());
        assert!(loaded.command_types.is_empty());
        assert!(loaded.projections.is_empty());
        assert!(loaded.recent_events.is_empty());
    });
}
