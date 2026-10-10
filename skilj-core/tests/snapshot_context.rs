//! Tests for `db::resolve_snapshot_context`/`db::catch_up_snapshots` -
//! [docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 2". Same "test the layer in
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
    let url = skilj_test_support::database_url("skilj_snapshot_context_test").await?;
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
    Some(TestDb { pool })
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
            correlation_id: None,
            causation_id: None,
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

/// Investigation prompted by scoping partitioned/parallel catch-up for
/// `Snapshot` (following Codeberg issue #25/docs/architecture.md §51's
/// `Projection` work): before building anything on top of `Snapshot`'s
/// existing `as_of_sequence` guard (`catch_up_snapshots`'s own
/// pre-fold check, mirroring the guard §50 had to *add* for
/// `Projection`), prove directly that it already holds under a real
/// concurrent race - no test in this file (or anywhere else in the
/// repo) had done so before this one. `tokio::join!`, the same
/// real-concurrency pattern `async_projections.rs`'s
/// `two_concurrent_instances_never_double_fold_the_same_event` uses:
/// two simultaneous `catch_up_snapshots` calls, simulating two
/// instances' own background pollers, against one account's `Deposited`
/// event. A double-fold would land the balance on `200`, not `100`.
#[test]
fn two_concurrent_instances_never_double_fold_the_same_snapshot_row() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let dispatcher = TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        };
        let account = unique_name("account");

        insert_money_event(&pool, &bc, &deposited, "account", &account, 100).await;

        let (r1, r2) = tokio::join!(
            db::catch_up_snapshots(&pool, &bc.name, &dispatcher),
            db::catch_up_snapshots(&pool, &bc.name, &dispatcher),
        );
        r1.unwrap();
        r2.unwrap();

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
        let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
        assert_eq!(state.balance, 100, "double-fold detected!");
    });
}

/// Codeberg issue #25 (docs/architecture.md §52) -
/// `TestSnapshotDispatcher`'s own partitioned twin, delegating
/// everything except `partition_count` so the tests below don't need
/// to touch `TestSnapshotDispatcher` or any of its existing
/// construction sites at all (the trait's own default `partition_count`,
/// `None` i.e. unpartitioned, already covers every test above
/// unchanged).
struct PartitionedTestSnapshotDispatcher {
    inner: TestSnapshotDispatcher,
    partition_count: u32,
}

impl SnapshotDispatcher for PartitionedTestSnapshotDispatcher {
    fn snapshot_names(&self, bounded_context: &str) -> Vec<&'static str> {
        self.inner.snapshot_names(bounded_context)
    }

    fn tag_key(&self, bounded_context: &str, snapshot_name: &str) -> Option<&'static str> {
        self.inner.tag_key(bounded_context, snapshot_name)
    }

    fn owner_tag_key(
        &self,
        bounded_context: &str,
        snapshot_name: &str,
    ) -> Option<Option<&'static str>> {
        self.inner.owner_tag_key(bounded_context, snapshot_name)
    }

    fn version(&self, bounded_context: &str, snapshot_name: &str) -> Option<u64> {
        self.inner.version(bounded_context, snapshot_name)
    }

    fn fold(
        &self,
        bounded_context: &str,
        snapshot_name: &str,
        state_json: &str,
        event: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        self.inner
            .fold(bounded_context, snapshot_name, state_json, event)
    }

    fn default_state(&self, bounded_context: &str, snapshot_name: &str) -> Option<String> {
        self.inner.default_state(bounded_context, snapshot_name)
    }

    fn partition_count(&self, _bounded_context: &str, snapshot_name: &str) -> Option<u32> {
        (snapshot_name == "Balance").then_some(self.partition_count)
    }
}

/// Codeberg issue #25 - the `Snapshot` twin of
/// `partitioned_projection_concurrent_instances_fold_every_key_exactly_once`:
/// three concurrent instances (`tokio::join!`), `PARTITION_COUNT = 4`,
/// three distinct accounts (tag values). Every account's final balance
/// must be exactly its own deposits/withdrawals' net - no loss, no
/// double-fold.
#[test]
fn partitioned_snapshot_concurrent_instances_fold_every_tag_value_exactly_once() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let dispatcher = PartitionedTestSnapshotDispatcher {
            inner: TestSnapshotDispatcher {
                tag_key: "account",
                version: 1,
            },
            partition_count: 4,
        };
        let acc_a = unique_name("account");
        let acc_b = unique_name("account");
        let acc_c = unique_name("account");

        insert_money_event(&pool, &bc, &deposited, "account", &acc_a, 10).await;
        insert_money_event(&pool, &bc, &deposited, "account", &acc_b, 20).await;
        insert_money_event(&pool, &bc, &deposited, "account", &acc_a, 5).await;
        insert_money_event(&pool, &bc, &deposited, "account", &acc_c, 7).await;
        insert_money_event(&pool, &bc, &deposited, "account", &acc_b, 3).await;
        insert_money_event(&pool, &bc, &deposited, "account", &acc_c, 1).await;

        let (r1, r2, r3) = tokio::join!(
            db::catch_up_snapshots(&pool, &bc.name, &dispatcher),
            db::catch_up_snapshots(&pool, &bc.name, &dispatcher),
            db::catch_up_snapshots(&pool, &bc.name, &dispatcher),
        );
        r1.unwrap();
        r2.unwrap();
        r3.unwrap();

        // A partition a losing instance's tick never claimed simply
        // isn't caught up yet after just one round - a second tick,
        // standing in for the next scheduled poll, is what a real fleet
        // would give it anyway.
        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .unwrap();

        for (account, expected) in [(&acc_a, 15), (&acc_b, 23), (&acc_c, 8)] {
            let resolved = db::resolve_snapshot_context(
                &pool,
                &bc.name,
                &dispatcher,
                "Balance",
                &[tag("account", account)],
            )
            .await
            .unwrap()
            .expect("a real row now exists");
            let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
            assert_eq!(state.balance, expected, "wrong balance for {account}");
        }
    });
}

/// Codeberg issue #25 - proves a partitioned snapshot's own per-row
/// `as_of_sequence` reaches the latest committed sequence across several
/// ticks interleaved with new events, mirroring
/// `partitioned_projection_caught_up_to_never_regresses_and_reaches_latest`.
#[test]
fn partitioned_snapshot_as_of_sequence_reaches_latest_across_ticks() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let dispatcher = PartitionedTestSnapshotDispatcher {
            inner: TestSnapshotDispatcher {
                tag_key: "account",
                version: 1,
            },
            partition_count: 4,
        };
        let account = unique_name("account");

        insert_money_event(&pool, &bc, &deposited, "account", &account, 1).await;
        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .unwrap();

        let last_seq = insert_money_event(&pool, &bc, &deposited, "account", &account, 2).await;
        for _ in 0..3 {
            db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
                .await
                .unwrap();
        }

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
        assert_eq!(state.balance, 3);
    });
}

/// Codeberg issue #25 - `PARTITION_COUNT = 4` with only one real tag
/// value touched: three of the four partitions have no matching tag
/// value in any event, proving idle partitions still advance their own
/// progress (so the rollup can still reach `latest`) purely from
/// scanning the batch, mirroring
/// `partitioned_projection_uneven_key_distribution_converges`.
#[test]
fn partitioned_snapshot_uneven_tag_distribution_converges() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let dispatcher = PartitionedTestSnapshotDispatcher {
            inner: TestSnapshotDispatcher {
                tag_key: "account",
                version: 1,
            },
            partition_count: 4,
        };
        let account = unique_name("account");

        insert_money_event(&pool, &bc, &deposited, "account", &account, 4).await;
        insert_money_event(&pool, &bc, &deposited, "account", &account, 6).await;
        let last_seq = insert_money_event(&pool, &bc, &deposited, "account", &account, 1).await;

        for _ in 0..2 {
            db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
                .await
                .unwrap();
        }

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
        assert_eq!(state.balance, 11);
    });
}

/// docs/architecture.md §152: after a `Snapshot::VERSION` bump, the next
/// event for a tag value used to reset that row to the default state and
/// fold *only that event* into it, at the new version - so the row claimed
/// to be current as of that event while having seen nothing before it,
/// and `decide_from_snapshot` (which trusts a row at the current version
/// and fetches only what came after it) decided on that. Whatever the row
/// holds afterwards, it must never be a current-version state that has
/// lost the history: either absent (a full replay) or the whole balance.
#[test]
fn a_version_bump_never_leaves_a_current_row_missing_history() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let withdrawn = seed_event_type(&pool, &bc, "Withdrawn", "account").await;
        let account = unique_name("account");
        let v1 = TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        };
        insert_money_event(&pool, &bc, &deposited, "account", &account, 100).await;
        insert_money_event(&pool, &bc, &withdrawn, "account", &account, 30).await;
        db::catch_up_snapshots(&pool, &bc.name, &v1).await.unwrap();

        // The model changed; the next deposit arrives and is caught up at
        // the new version.
        let v2 = TestSnapshotDispatcher {
            tag_key: "account",
            version: 2,
        };
        insert_money_event(&pool, &bc, &deposited, "account", &account, 5).await;
        for _ in 0..3 {
            db::catch_up_snapshots(&pool, &bc.name, &v2).await.unwrap();
        }

        let resolved = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &v2,
            "Balance",
            &[tag("account", &account)],
        )
        .await
        .unwrap()
        .unwrap();
        let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
        assert!(
            resolved.as_of_sequence == -1 || state.balance == 75,
            "a current-version row must hold the whole history (75) or be absent, got \
             balance {} as of {}",
            state.balance,
            resolved.as_of_sequence
        );
    });
}

/// `a_version_bump_never_leaves_a_current_row_missing_history`, through
/// the partitioned catch-up (`Snapshot::PARTITION_COUNT` > 1), which
/// resets and folds rows the same way.
#[test]
fn a_version_bump_never_leaves_a_partitioned_row_missing_history() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let withdrawn = seed_event_type(&pool, &bc, "Withdrawn", "account").await;
        let account = unique_name("account");
        let at = |version| PartitionedTestSnapshotDispatcher {
            inner: TestSnapshotDispatcher {
                tag_key: "account",
                version,
            },
            partition_count: 4,
        };
        insert_money_event(&pool, &bc, &deposited, "account", &account, 100).await;
        insert_money_event(&pool, &bc, &withdrawn, "account", &account, 30).await;
        db::catch_up_snapshots(&pool, &bc.name, &at(1))
            .await
            .unwrap();

        insert_money_event(&pool, &bc, &deposited, "account", &account, 5).await;
        for _ in 0..3 {
            db::catch_up_snapshots(&pool, &bc.name, &at(2))
                .await
                .unwrap();
        }

        let resolved = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &at(2),
            "Balance",
            &[tag("account", &account)],
        )
        .await
        .unwrap()
        .unwrap();
        let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
        assert!(
            resolved.as_of_sequence == -1 || state.balance == 75,
            "got balance {} as of {}",
            state.balance,
            resolved.as_of_sequence
        );
    });
}

/// docs/architecture.md §197: one tick folds up to 1000 events, 100 per
/// transaction. 250 events over seven accounts - new ones and ones a
/// previous tick already holds - take three chunks, and every row must
/// end with each of its events folded exactly once, as of its last one.
#[test]
fn a_chunked_catch_up_folds_every_event_once_across_chunks() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let dispatcher = TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        };
        let accounts: Vec<String> = (0..7).map(|_| unique_name("account")).collect();
        for account in &accounts[..3] {
            insert_money_event(&pool, &bc, &deposited, "account", account, 1000).await;
        }
        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .unwrap();

        let mut last = std::collections::HashMap::new();
        for i in 0..250 {
            let account = &accounts[i % accounts.len()];
            let seq = insert_money_event(&pool, &bc, &deposited, "account", account, 1).await;
            last.insert(account.clone(), seq);
        }
        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .unwrap();

        for (i, account) in accounts.iter().enumerate() {
            let resolved = db::resolve_snapshot_context(
                &pool,
                &bc.name,
                &dispatcher,
                "Balance",
                &[tag("account", account)],
            )
            .await
            .unwrap()
            .unwrap();
            let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
            let deposits = (250 / accounts.len() + usize::from(i < 250 % accounts.len())) as i64;
            let opening = if i < 3 { 1000 } else { 0 };
            assert_eq!(state.balance, opening + deposits, "balance of account {i}");
            assert_eq!(
                resolved.as_of_sequence, last[account],
                "position of account {i}"
            );
        }
    });
}

/// `TestSnapshotDispatcher`, failing to fold any `Poison` event.
struct PoisonedTestSnapshotDispatcher(TestSnapshotDispatcher);

impl SnapshotDispatcher for PoisonedTestSnapshotDispatcher {
    fn snapshot_names(&self, bounded_context: &str) -> Vec<&'static str> {
        self.0.snapshot_names(bounded_context)
    }

    fn tag_key(&self, bounded_context: &str, snapshot_name: &str) -> Option<&'static str> {
        self.0.tag_key(bounded_context, snapshot_name)
    }

    fn owner_tag_key(
        &self,
        bounded_context: &str,
        snapshot_name: &str,
    ) -> Option<Option<&'static str>> {
        self.0.owner_tag_key(bounded_context, snapshot_name)
    }

    fn version(&self, bounded_context: &str, snapshot_name: &str) -> Option<u64> {
        self.0.version(bounded_context, snapshot_name)
    }

    fn fold(
        &self,
        bounded_context: &str,
        snapshot_name: &str,
        state_json: &str,
        event: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        if event.event_type.name == "Poison" {
            return Some(Err(skilj_core::event_store::Error::PayloadDecodeFailed(
                "poison".to_string(),
            )
            .into()));
        }
        self.0
            .fold(bounded_context, snapshot_name, state_json, event)
    }

    fn default_state(&self, bounded_context: &str, snapshot_name: &str) -> Option<String> {
        self.0.default_state(bounded_context, snapshot_name)
    }
}

/// docs/architecture.md §197: with one transaction per event, an event
/// whose fold fails left every event before it committed. A chunk keeps
/// that: the events before the failing one are folded again on their
/// own and committed, and the error is returned.
#[test]
fn a_failing_event_mid_chunk_keeps_the_progress_before_it() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let poison = seed_event_type(&pool, &bc, "Poison", "account").await;
        let dispatcher = PoisonedTestSnapshotDispatcher(TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        });
        let account = unique_name("account");
        let other = unique_name("account");
        insert_money_event(&pool, &bc, &deposited, "account", &account, 10).await;
        insert_money_event(&pool, &bc, &deposited, "account", &other, 7).await;
        let before = insert_money_event(&pool, &bc, &deposited, "account", &account, 5).await;
        insert_money_event(&pool, &bc, &poison, "account", &account, 0).await;
        insert_money_event(&pool, &bc, &deposited, "account", &account, 1).await;

        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .expect_err("the poison event fails its fold");

        let resolved = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &dispatcher,
            "Balance",
            &[tag("account", &account)],
        )
        .await
        .unwrap()
        .unwrap();
        let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
        assert_eq!(state.balance, 15);
        assert_eq!(resolved.as_of_sequence, before);
        let resolved = db::resolve_snapshot_context(
            &pool,
            &bc.name,
            &dispatcher,
            "Balance",
            &[tag("account", &other)],
        )
        .await
        .unwrap()
        .unwrap();
        let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
        assert_eq!(state.balance, 7);
    });
}

/// `insert_money_event` with the event's tags given in full, so one event
/// can carry several values of the snapshot's tag key.
async fn insert_tagged_event(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    tags: Vec<Tag>,
    amount: i64,
) -> i64 {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let e = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: serde_json::json!({ "amount": amount }).to_string(),
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
    seq
}

/// A `VERSION` bump refolds a row over its whole history, read a page of
/// 1000 events at a time from `event_tags`, one row per tag. Here every
/// event carries two values of the snapshot's key - a transfer between
/// two accounts - so the history has twice as many tag rows as events.
/// Counting rows ended the paging after the first page, and the rows were
/// marked current while missing the rest. An event folds into the first
/// value of the key it carries, as the per-chunk fold always did, so each
/// account holds the events that list it first.
#[test]
fn a_version_bump_refolds_every_page_when_events_carry_two_values() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let transferred = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let a = unique_name("account");
        let b = unique_name("account");
        let at = |version| TestSnapshotDispatcher {
            tag_key: "account",
            version,
        };
        for i in 0..1200 {
            let tags = if i % 2 == 0 {
                vec![tag("account", &a), tag("account", &b)]
            } else {
                vec![tag("account", &b), tag("account", &a)]
            };
            insert_tagged_event(&pool, &bc, &transferred, tags, 1).await;
        }
        for _ in 0..3 {
            db::catch_up_snapshots(&pool, &bc.name, &at(1))
                .await
                .unwrap();
        }

        insert_tagged_event(
            &pool,
            &bc,
            &transferred,
            vec![tag("account", &a), tag("account", &b)],
            1,
        )
        .await;
        let last = insert_tagged_event(
            &pool,
            &bc,
            &transferred,
            vec![tag("account", &b), tag("account", &a)],
            1,
        )
        .await;
        for _ in 0..3 {
            db::catch_up_snapshots(&pool, &bc.name, &at(2))
                .await
                .unwrap();
        }

        for account in [&a, &b] {
            let resolved = db::resolve_snapshot_context(
                &pool,
                &bc.name,
                &at(2),
                "Balance",
                &[tag("account", account)],
            )
            .await
            .unwrap()
            .unwrap();
            let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
            assert_eq!(state.balance, 601, "every event listing it first");
            assert!(resolved.as_of_sequence >= last - 1);
        }
    });
}

/// A chunk can fail at a later event before an earlier one: a new row's
/// history is refolded before the other rows are folded, so its failing
/// event is met first. The run before the later failure then fails again
/// at the earlier one, and is shortened again, so the events before the
/// earliest failure still commit (docs/architecture.md §200). Here `a` is
/// new and fails at index 10, `b` is held and fails at index 5.
#[test]
fn the_earliest_of_two_failing_events_bounds_the_committed_progress() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let deposited = seed_event_type(&pool, &bc, "Deposited", "account").await;
        let poison = seed_event_type(&pool, &bc, "Poison", "account").await;
        let dispatcher = PoisonedTestSnapshotDispatcher(TestSnapshotDispatcher {
            tag_key: "account",
            version: 1,
        });
        let a = unique_name("account");
        let b = unique_name("account");
        insert_money_event(&pool, &bc, &deposited, "account", &b, 100).await;
        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .unwrap();

        let mut sequences = Vec::new();
        for (i, (account, event_type)) in [
            (&b, &deposited),
            (&a, &deposited),
            (&b, &deposited),
            (&a, &deposited),
            (&b, &deposited),
            (&b, &poison),
            (&a, &deposited),
            (&a, &deposited),
            (&a, &deposited),
            (&a, &deposited),
            (&a, &poison),
            (&a, &deposited),
        ]
        .into_iter()
        .enumerate()
        {
            sequences.push(
                insert_money_event(&pool, &bc, event_type, "account", account, i as i64 + 1).await,
            );
        }

        db::catch_up_snapshots(&pool, &bc.name, &dispatcher)
            .await
            .expect_err("the poison events fail their folds");

        let balance = |account: String| {
            let pool = pool.clone();
            let bc = bc.name.clone();
            let dispatcher = &dispatcher;
            async move {
                let resolved = db::resolve_snapshot_context(
                    &pool,
                    &bc,
                    dispatcher,
                    "Balance",
                    &[tag("account", &account)],
                )
                .await
                .unwrap()
                .unwrap();
                let state: BalanceState = serde_json::from_str(&resolved.state_json).unwrap();
                (state.balance, resolved.as_of_sequence)
            }
        };
        assert_eq!(balance(b.clone()).await, (100 + 1 + 3 + 5, sequences[4]));
        assert_eq!(balance(a.clone()).await, (2 + 4, sequences[3]));
    });
}
