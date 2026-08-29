//! Shared integration-test harness for `tests/banking.rs`/`tests/
//! courses.rs` - not its own test binary (it lives under `tests/support/`,
//! not directly under `tests/`, so cargo doesn't compile it as one; each
//! of the two real test files pulls it in via `mod support;`). Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip provisioning and
//! direct-`db::`-seeding bootstrap every end-to-end test elsewhere in
//! this repo already uses (see e.g. `skilj/tests/command_trigger.rs`'s
//! own doc comment) - factored into one place here since this crate,
//! unlike the rest of the workspace, doesn't need to keep each test file
//! independently copy-pasteable.
#![allow(dead_code)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, SubsecRound, Utc};
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use skilj::Skilj;
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};
use tower::ServiceExt;

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

pub struct TestDb {
    pub database_url: String,
    pub pool: Pool,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

pub fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for skilj-demo tests")
    })
}

pub async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| (db.database_url.clone(), db.pool.clone()))
}

async fn connect_and_migrate(database_url: &str, label: &str) -> Option<Pool> {
    let pool = match db::connect(database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to {label} failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating {label} failed: {e}");
        return None;
    }
    Some(pool)
}

async fn provision() -> Option<TestDb> {
    let database_url = if let Ok(database_url) = std::env::var("DATABASE_URL") {
        database_url
    } else {
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
        let database_name = "skilj_demo_test";
        if let Err(e) = server.create_database(database_name).await {
            eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
            return None;
        }
        let url = server.settings().url(database_name);
        let pool = connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            pool,
            _embedded: Some(server),
        });
    };

    let pool = connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb {
        database_url,
        pool,
        _embedded: None,
    })
}

pub fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

pub fn test_now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

// --- bootstrap: seed an admin Role with access to both bounded contexts ---

/// `skilj_demo::register()` always registers under the fixed names
/// `skilj_demo::banking::BOUNDED_CONTEXT`/`courses::BOUNDED_CONTEXT`, so
/// (unlike every other test file in this workspace) this harness can't
/// uniquify the bounded context itself per test - only the entity ids
/// inside it (`unique_name("account")` etc). Creating each bounded
/// context row is therefore done at most once per test binary, guarded
/// by its own `OnceCell` so two `#[test]`s racing to seed it can't
/// double-insert.
static BOUNDED_CONTEXTS_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

async fn ensure_bounded_contexts(pool: &Pool) {
    BOUNDED_CONTEXTS_READY
        .get_or_init(|| async {
            for name in [
                skilj_demo::banking::BOUNDED_CONTEXT,
                skilj_demo::courses::BOUNDED_CONTEXT,
            ] {
                if db::get_bounded_context(pool, name).await.unwrap().is_none() {
                    db::insert_bounded_context(
                        pool,
                        &BoundedContext {
                            name: name.to_string(),
                            status: BoundedContextStatus::Active,
                            created_at: test_now(),
                            created_by: ContextCreator::SystemCreator,
                            template: None,
                        },
                    )
                    .await
                    .unwrap();
                }
            }
        })
        .await;
}

/// A fresh admin `Role`, granted `Admin` access to both bounded contexts.
/// Safe to call once per `#[test]` (each gets its own `Role`, so unlike
/// `ensure_bounded_contexts` there's nothing to deduplicate).
pub async fn seed_admin(pool: &Pool) -> Vec<RoleAccessMapping> {
    ensure_bounded_contexts(pool).await;

    let external_subject = unique_name("subject");
    let role = Role {
        id: generate_token_id(),
        external_subject,
        name: "Test Admin".into(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();

    let mut mappings = Vec::new();
    for name in [
        skilj_demo::banking::BOUNDED_CONTEXT,
        skilj_demo::courses::BOUNDED_CONTEXT,
    ] {
        let bounded_context = db::get_bounded_context(pool, name)
            .await
            .unwrap()
            .expect("ensure_bounded_contexts just made sure this exists");
        let mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(pool, &mapping)
            .await
            .unwrap();
        mappings.push(mapping);
    }
    mappings
}

/// A fully-built `Skilj` with both `skilj_demo` bounded contexts
/// reconciled, plus the `RoleAccessMapping`s reconciliation used - a
/// `#[test]` mints its own `CommandToken`s from the mapping matching
/// whichever bounded context it needs (see `mint_command_token`).
pub async fn setup() -> (Skilj, Pool, Vec<RoleAccessMapping>) {
    let (database_url, pool) = test_db()
        .await
        .expect("test_db() must be Some - caller already checked");
    let mappings = seed_admin(&pool).await;
    let external_subject = mappings[0].role.external_subject.clone();

    let (skilj, report) = skilj_demo::register(Skilj::builder(database_url))
        .reconciliation_role(external_subject)
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    (skilj, pool, mappings)
}

pub fn mapping_for<'a>(
    mappings: &'a [RoleAccessMapping],
    bounded_context: &str,
) -> &'a RoleAccessMapping {
    mappings
        .iter()
        .find(|m| m.bounded_context.name == bounded_context)
        .expect("setup() seeds a mapping for every skilj_demo bounded context")
}

pub async fn mint_command_token(
    pool: &Pool,
    mapping: &RoleAccessMapping,
    bounded_context: &str,
    command_type_name: &str,
) -> String {
    let command_type = db::get_command_type(pool, bounded_context, command_type_name)
        .await
        .unwrap()
        .unwrap_or_else(|| {
            panic!("{bounded_context}/{command_type_name} must already be registered")
        });
    let token = access_control::create_command_token(
        mapping,
        &command_type,
        generate_token_id(),
        generate_token_secret(),
        test_now(),
    )
    .unwrap();
    db::insert_command_token(pool, &token).await.unwrap();
    format!("{}.{}", token.id, token.secret)
}

// --- HTTP: POST /v1/commands/trigger ---

/// Triggers `payload` against `/v1/commands/trigger` with `credential`
/// and returns the decoded JSON response - always `200`, since a business
/// rejection is a legitimate outcome carried in the body
/// (`{"accepted": false, ...}`), not an HTTP error (docs/architecture.md
/// §5.4/§7.3). Clones `router` internally so a `#[test]` can call this
/// more than once against the same `Skilj::rest_router()`.
pub async fn trigger(
    router: &axum::Router,
    credential: &str,
    payload: serde_json::Value,
) -> serde_json::Value {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/commands/trigger")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({ "payload": payload })).unwrap(),
        ))
        .unwrap();

    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a well-formed CommandTrigger request always renders 200, accepted or not"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

pub fn accepted(response: &serde_json::Value) -> bool {
    response["accepted"]
        .as_bool()
        .expect("CommandTriggerResponse.accepted is always present")
}

pub fn rejection_kind(response: &serde_json::Value) -> &str {
    response["rejectionKind"]
        .as_str()
        .expect("a rejected response always carries rejectionKind")
}

// --- projection reads ---

pub async fn projection_state<T: DeserializeOwned + Default>(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    key: &str,
) -> T {
    match db::get_projection_state(pool, bounded_context, projection_name, key)
        .await
        .unwrap()
    {
        Some(json) => serde_json::from_str(&json).unwrap(),
        None => T::default(),
    }
}
