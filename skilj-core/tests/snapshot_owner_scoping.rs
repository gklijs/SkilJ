//! Tests for the snapshot half of the cross-tenant read fix
//! (docs/architecture.md's own write-up of these passes) - the fifth and
//! final surface, after `ProjectionQuery`, raw events, `CommandQuery`,
//! and `TypeRegistration`'s own read-back. `SnapshotInspection`
//! (`inspectSnapshot`) shared the identical gap: `AdminAccess` alone,
//! with no check that a queried `tag_value`'s own row belonged to the
//! caller.
//!
//! `Snapshot::OWNER_TAG_KEY` is deliberately Rust-only, the same
//! treatment `TAG_KEY` itself already gets (no spec field, no
//! registration surface - `Snapshot` is compiled, deployed configuration
//! throughout). This file proves the DB-level half: `db::catch_up_snapshots`
//! derives and persists a stored row's own `owner` from the folding
//! event's own tags, and `db::get_snapshot_state_and_owner` reads it
//! back. `access_control::scope_matches_owner` (the pure predicate
//! `inspectSnapshot`'s resolver applies) is tested in isolation too. Same
//! "test the DB/dispatcher plumbing in isolation" shape
//! `snapshot_context.rs` already uses - see its own doc comment for the
//! shared provisioning harness this file duplicates rather than extracts
//! (the same call every prior pass in this project already made). The
//! resolver's own rejection - the GraphQL-facing half - is proven end to
//! end in `skilj/tests/graphql_business_surfaces.rs`.

use chrono::{SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use skilj_core::access_control;
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::SnapshotDispatcher;
use skilj_core::shared::{generate_token_id, Metadata, Tag, TagMapping};

#[derive(Debug, Default, Serialize, Deserialize)]
struct TotalState {
    total: i64,
}

/// One registered snapshot, `"Total"`, keyed by `"account"` -
/// `owner_tag_key` is a plain field (not hardcoded) so a test can
/// construct an owner-scoped and an unscoped dispatcher over the same
/// stored data, the same "vary one field, not the compiled type" shape
/// `snapshot_context.rs`'s own `TestSnapshotDispatcher` already uses for
/// `version`.
struct TestSnapshotDispatcher {
    owner_tag_key: Option<&'static str>,
}

impl SnapshotDispatcher for TestSnapshotDispatcher {
    fn snapshot_names(&self, _bounded_context: &str) -> Vec<&'static str> {
        vec!["Total"]
    }

    fn tag_key(&self, _bounded_context: &str, snapshot_name: &str) -> Option<&'static str> {
        (snapshot_name == "Total").then_some("account")
    }

    fn owner_tag_key(
        &self,
        _bounded_context: &str,
        snapshot_name: &str,
    ) -> Option<Option<&'static str>> {
        (snapshot_name == "Total").then_some(self.owner_tag_key)
    }

    fn version(&self, _bounded_context: &str, snapshot_name: &str) -> Option<u64> {
        (snapshot_name == "Total").then_some(1)
    }

    fn fold(
        &self,
        _bounded_context: &str,
        snapshot_name: &str,
        state_json: &str,
        event: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        if snapshot_name != "Total" {
            return None;
        }
        let mut state: TotalState = match serde_json::from_str(state_json) {
            Ok(s) => s,
            Err(e) => {
                return Some(Err(skilj_core::event_store::Error::PayloadDecodeFailed(
                    e.to_string(),
                )
                .into()))
            }
        };
        let amount: i64 = serde_json::from_str::<serde_json::Value>(&event.payload)
            .ok()
            .and_then(|v| v.get("amount").and_then(|a| a.as_i64()))
            .unwrap_or(0);
        state.total += amount;
        Some(Ok(serde_json::to_string(&state).unwrap()))
    }

    fn default_state(&self, _bounded_context: &str, snapshot_name: &str) -> Option<String> {
        (snapshot_name == "Total").then(|| serde_json::to_string(&TotalState::default()).unwrap())
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---
// Identical harness to `snapshot_context.rs` - see its own doc comment.

struct TestDb {
    pool: Pool,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for snapshot_owner_scoping tests")
    })
}

async fn test_pool() -> Option<Pool> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.pool.clone())
}

async fn provision() -> Option<TestDb> {
    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        let pool = match db::connect(&database_url).await {
            Ok(pool) => pool,
            Err(e) => {
                eprintln!("skipping: DATABASE_URL is set but connecting failed: {e}");
                return None;
            }
        };
        if let Err(e) = db::migrate(&pool).await {
            eprintln!("skipping: DATABASE_URL migration failed: {e}");
            return None;
        }
        return Some(TestDb {
            pool,
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
    let database_name = "skilj_snapshot_owner_scoping_test";
    if let Err(e) = server.create_database(database_name).await {
        eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
        return None;
    }
    let pool = match db::connect(&server.settings().url(database_name)).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to embedded PostgreSQL failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating embedded PostgreSQL failed: {e}");
        return None;
    }
    Some(TestDb {
        pool,
        _embedded: Some(server),
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn seed_bounded_context(pool: &Pool) -> BoundedContext {
    let bc = BoundedContext {
        name: unique_name("bc"),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    bc
}

/// Tagged both `"account"` (`Total`'s own `TAG_KEY`) and `"company"` (a
/// separate owner dimension) - proving the fold-time derivation reads a
/// tag key that need not equal the snapshot's own `TAG_KEY`.
async fn seed_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "Deposited".to_string(),
        schema: r#"{"properties":{"account_id":{"type":"string"},"company_id":{"type":"string"},"amount":{"type":"integer"}}}"#
            .to_string(),
        schema_version: 1,
        tag_mappings: vec![
            TagMapping {
                key: "account".to_string(),
                field: "account_id".to_string(),
            },
            TagMapping {
                key: "company".to_string(),
                field: "company_id".to_string(),
            },
        ],
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: true,
        direct_creation_allowed: true,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: true,
    };
    db::upsert_event_type(pool, &et).await.unwrap();
    et
}

async fn insert_deposit(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    account: &str,
    company: Option<&str>,
    amount: i64,
) {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let mut tags = vec![Tag {
        key: "account".to_string(),
        value: Some(account.to_string()),
    }];
    if let Some(company) = company {
        tags.push(Tag {
            key: "company".to_string(),
            value: Some(company.to_string()),
        });
    }
    let e = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: serde_json::json!({
            "account_id": account,
            "company_id": company,
            "amount": amount,
        })
        .to_string(),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: "test".to_string(),
            created_at: test_now(),
            correlation_id: None,
            causation_id: None,
        },
        sequence: seq,
        tags,
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event(pool, &e, None).await.unwrap();
}

// ---------------------------------------------------------------------
// access_control::scope_matches_owner - in isolation
// ---------------------------------------------------------------------

#[test]
fn scope_matches_owner_when_scope_is_none() {
    assert!(access_control::scope_matches_owner(Some("company-b"), None));
    assert!(access_control::scope_matches_owner(None, None));
}

#[test]
fn scope_matches_owner_when_they_agree() {
    assert!(access_control::scope_matches_owner(
        Some("company-a"),
        Some("company-a")
    ));
}

#[test]
fn scope_does_not_match_a_different_owner() {
    assert!(!access_control::scope_matches_owner(
        Some("company-b"),
        Some("company-a")
    ));
}

/// Fail-closed: an unestablished owner is treated the same as a proven
/// mismatch, not as "no conflict" - the identical stance every sibling
/// predicate in this fix already takes.
#[test]
fn scope_does_not_match_an_unestablished_owner() {
    assert!(!access_control::scope_matches_owner(
        None,
        Some("company-a")
    ));
}

// ---------------------------------------------------------------------
// catch_up_snapshots - derives and persists the owner column
// ---------------------------------------------------------------------

#[test]
fn catch_up_snapshots_derives_the_owner_from_a_tag_key_other_than_tag_key_itself() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let dispatcher = TestSnapshotDispatcher {
            owner_tag_key: Some("company"),
        };
        let account = unique_name("account");

        insert_deposit(&pool, &bc, &et, &account, Some("company-a"), 100).await;

        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .unwrap();

        let (_, owner) =
            db::get_snapshot_state_and_owner(&pool, &bc.name, "Total", "account", &account)
                .await
                .unwrap()
                .unwrap();

        assert_eq!(owner.as_deref(), Some("company-a"));
    });
}

#[test]
fn catch_up_snapshots_derives_no_owner_when_the_snapshot_declares_no_owner_dimension() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let dispatcher = TestSnapshotDispatcher {
            owner_tag_key: None,
        };
        let account = unique_name("account");

        insert_deposit(&pool, &bc, &et, &account, Some("company-a"), 100).await;

        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .unwrap();

        let (_, owner) =
            db::get_snapshot_state_and_owner(&pool, &bc.name, "Total", "account", &account)
                .await
                .unwrap()
                .unwrap();

        assert_eq!(owner, None);
    });
}

/// A later fold from an event carrying no `company` tag leaves an
/// already-established owner untouched - `apply_projection_fold_update`'s
/// own identical "an event lacking the tag never clears an owner"
/// contract, applied here.
#[test]
fn catch_up_snapshots_leaves_an_established_owner_untouched_when_a_later_event_lacks_the_tag() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let dispatcher = TestSnapshotDispatcher {
            owner_tag_key: Some("company"),
        };
        let account = unique_name("account");

        insert_deposit(&pool, &bc, &et, &account, Some("company-a"), 100).await;
        insert_deposit(&pool, &bc, &et, &account, None, 50).await;

        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .unwrap();

        let ((_, as_of_sequence, state_json, _), owner) =
            db::get_snapshot_state_and_owner(&pool, &bc.name, "Total", "account", &account)
                .await
                .unwrap()
                .unwrap();

        assert!(as_of_sequence >= 1); // both events folded
        let state: TotalState = serde_json::from_str(&state_json).unwrap();
        assert_eq!(state.total, 150);
        assert_eq!(owner.as_deref(), Some("company-a")); // owner survived
    });
}

#[test]
fn get_snapshot_state_and_owner_is_none_for_a_never_touched_tag_value() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;

        assert!(db::get_snapshot_state_and_owner(
            &pool,
            &bc.name,
            "Total",
            "account",
            "never-touched"
        )
        .await
        .unwrap()
        .is_none());
    });
}
