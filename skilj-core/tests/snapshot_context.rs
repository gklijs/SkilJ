//! Tests for `db::resolve_snapshot_context`/`db::catch_up_snapshots` -
//! docs/architecture.md §19's "Problem 2". Same "test the layer in
//! isolation" shape `skilj-core/tests/submit_command.rs`/`tag_indexed_events.rs`
//! already use - see either file's own doc comment for the harness
//! details, not repeated a third time here. `skilj-demo/tests/snapshot.rs`
//! is the real end-to-end proof (a genuine `Snapshot`/`CommandType`
//! adopter, real HTTP, a tampered stored row proving `decide_from_snapshot`
//! is actually read from, not silently bypassed) - this file is the
//! narrower unit-level one, for the DB/dispatcher plumbing underneath it.

use chrono::{SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::SnapshotDispatcher;
use skilj_core::shared::{generate_token_id, Metadata, Tag, TagMapping};

#[derive(Debug, Default, Serialize, Deserialize)]
struct BalanceState {
    balance: i64,
}

/// One registered snapshot, `"Balance"`, scoped to `tag_key` - a plain
/// struct field rather than a hardcoded `"account"` so
/// `a_stored_row_at_an_old_version_is_treated_as_absent` can construct a
/// second dispatcher instance at a different `version` over the same
/// stored data, proving the version check without a second compiled
/// `Snapshot` type.
struct TestSnapshotDispatcher {
    tag_key: &'static str,
    version: u64,
}

impl SnapshotDispatcher for TestSnapshotDispatcher {
    fn snapshot_names(&self, _bounded_context: &str) -> Vec<&'static str> {
        vec!["Balance"]
    }

    fn tag_key(&self, _bounded_context: &str, snapshot_name: &str) -> Option<&'static str> {
        (snapshot_name == "Balance").then_some(self.tag_key)
    }

    fn owner_tag_key(
        &self,
        _bounded_context: &str,
        snapshot_name: &str,
    ) -> Option<Option<&'static str>> {
        (snapshot_name == "Balance").then_some(None)
    }

    fn version(&self, _bounded_context: &str, snapshot_name: &str) -> Option<u64> {
        (snapshot_name == "Balance").then_some(self.version)
    }

    fn fold(
        &self,
        _bounded_context: &str,
        snapshot_name: &str,
        state_json: &str,
        event: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        if snapshot_name != "Balance" {
            return None;
        }
        let mut state: BalanceState = match serde_json::from_str(state_json) {
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
        match event.event_type.name.as_str() {
            "Deposited" => state.balance += amount,
            "Withdrawn" => state.balance -= amount,
            _ => {}
        }
        Some(Ok(serde_json::to_string(&state).unwrap()))
    }

    fn default_state(&self, _bounded_context: &str, snapshot_name: &str) -> Option<String> {
        (snapshot_name == "Balance")
            .then(|| serde_json::to_string(&BalanceState::default()).unwrap())
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    pool: Pool,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for snapshot_context tests")
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
    let database_name = "skilj_snapshot_context_test";
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

async fn seed_event_type(pool: &Pool, bc: &BoundedContext, name: &str, tag_key: &str) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: name.to_string(),
        schema: r#"{"properties":{"account_id":{"type":"string"},"amount":{"type":"integer"}}}"#
            .to_string(),
        schema_version: 1,
        tag_mappings: vec![TagMapping {
            key: tag_key.to_string(),
            field: "account_id".to_string(),
        }],
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

async fn insert_money_event(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    tag_key: &str,
    tag_value: &str,
    amount: i64,
) -> i64 {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let e = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: serde_json::json!({ "account_id": tag_value, "amount": amount }).to_string(),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: "test".to_string(),
            created_at: test_now(),
        },
        sequence: seq,
        tags: vec![Tag {
            key: tag_key.to_string(),
            value: Some(tag_value.to_string()),
        }],
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event(pool, &e, None).await.unwrap();
    seq
}

fn tag(key: &str, value: &str) -> Tag {
    Tag {
        key: key.to_string(),
        value: Some(value.to_string()),
    }
}

#[test]
fn falls_back_when_consistency_tags_dont_match_the_snapshots_tag_key() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let dispatcher = TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        };

        let resolved = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &dispatcher,
            "Balance",
            &[tag("order", "A")],
        )
        .await
        .unwrap();

        assert!(resolved.is_none());
    });
}

#[test]
fn falls_back_for_multi_tag_commands_even_when_one_tag_matches() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let dispatcher = TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        };

        let resolved = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &dispatcher,
            "Balance",
            &[tag("account", "A"), tag("order", "B")],
        )
        .await
        .unwrap();

        assert!(resolved.is_none());
    });
}

#[test]
fn resolves_to_a_cold_default_when_nothing_stored_yet() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let dispatcher = TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        };

        let resolved = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &dispatcher,
            "Balance",
            &[tag("account", "never-touched")],
        )
        .await
        .unwrap()
        .expect("a registered, tag-matching snapshot always resolves to Some");

        assert_eq!(resolved.as_of_sequence, -1);
        let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
        assert_eq!(state.balance, 0);
    });
}

#[test]
fn catch_up_snapshots_folds_real_events_and_resolve_snapshot_context_reads_them_back() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let withdrawn = seed_event_type(&pool, &bc, "Withdrawn", "account").await;
        let dispatcher = TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        };
        let account = unique_name("account");

        insert_money_event(&pool, &bc, &deposited, "account", &account, 100).await;
        let last_seq = insert_money_event(&pool, &bc, &withdrawn, "account", &account, 30).await;

        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .unwrap();

        let resolved = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &dispatcher,
            "Balance",
            &[tag("account", &account)],
        )
        .await
        .unwrap()
        .expect("a real row now exists");

        assert_eq!(resolved.as_of_sequence, last_seq);
        let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
        assert_eq!(state.balance, 70);
    });
}

#[test]
fn a_stored_row_at_an_old_version_is_treated_as_absent() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let account = unique_name("account");
        let dispatcher_v1 = TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        };

        insert_money_event(&pool, &bc, &deposited, "account", &account, 100).await;
        db::catch_up_snapshots(&pool, &bc.name, &dispatcher_v1)
            .await
            .unwrap();

        // Confirm the real row exists at version 1 first - otherwise a
        // failure below would be indistinguishable from "there was never
        // a row to begin with".
        let resolved_v1 = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &dispatcher_v1,
            "Balance",
            &[tag("account", &account)],
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            resolved_v1.as_of_sequence >= 0,
            "must be a real, non-cold row"
        );
        let state_v1: BalanceState = serde_json::from_str(&resolved_v1.state_json).unwrap();
        assert_eq!(state_v1.balance, 100);

        // The model changed - fold()'s own logic (or State's shape) is
        // now at version 2. The stored version-1 row must be treated as
        // if it doesn't exist, not read and misinterpreted.
        let dispatcher_v2 = TestSnapshotDispatcher {
            tag_key: "account",
            version: 2,
        };
        let resolved_v2 = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &dispatcher_v2,
            "Balance",
            &[tag("account", &account)],
        )
        .await
        .unwrap()
        .expect("a registered, tag-matching snapshot always resolves to Some");

        assert_eq!(
            resolved_v2.as_of_sequence, -1,
            "must be treated as cold, not the real stored 100"
        );
        let state_v2: BalanceState = serde_json::from_str(&resolved_v2.state_json).unwrap();
        assert_eq!(state_v2.balance, 0);
    });
}
