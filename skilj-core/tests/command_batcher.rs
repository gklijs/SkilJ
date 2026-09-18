//! Concurrency tests for `command_batcher::CommandBatcher` - see its own
//! module doc comment for the design this proves. `db::submit_command_batch`'s
//! own tests (`skilj-core/tests/submit_command.rs`) already prove the
//! batch machinery's correctness deterministically, against a hand-built
//! batch, single-threaded; this file proves the leader/follower
//! coalescing itself holds up under real concurrent tokio tasks racing
//! to submit at the same time, not just a batch this test constructed by
//! hand. Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `submit_command.rs` - see its own doc comment for the details, not
//! repeated a third time here.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::command_batcher::CommandBatcher;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, CommandType, Event, EventBroadcaster, EventType,
};
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher};
use skilj_core::shared::{generate_token_id, CommandDecision, EventSpec, TagMapping};
use std::sync::Arc;

/// The same "can't ship the same order twice" DCB-guarded decider
/// `submit_command.rs`'s own `TestCommandDispatcher` uses for its
/// `ShipOrder` case - rejects once anything already matches its own
/// consistency tag.
struct ShipOrderDispatcher;

impl CommandDispatcher for ShipOrderDispatcher {
    fn dispatch(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        payload: &str,
        matching_events: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        if matching_events.is_empty() {
            Some(Ok(CommandDecision::Accepted {
                events: vec![EventSpec {
                    event_type: "OrderShipped".to_string(),
                    payload: serde_json::from_str(payload).expect("test payload is always JSON"),
                }],
            }))
        } else {
            Some(Ok(CommandDecision::Rejected {
                reason: "already shipped".to_string(),
                kind: "already_shipped".to_string(),
            }))
        }
    }

    fn required_role(
        &self,
        _bounded_context: &str,
        _command_type: &str,
    ) -> Option<Option<&'static str>> {
        Some(None)
    }

    fn snapshot_name(
        &self,
        _bounded_context: &str,
        _command_type: &str,
    ) -> Option<Option<&'static str>> {
        Some(None)
    }

    fn dispatch_from_snapshot(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        _payload: &str,
        _snapshot_state_json: &str,
        _events_since_snapshot: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        unreachable!("this fixture never opts into snapshot()")
    }
}

/// No sync projections anywhere in these tests.
struct NoopProjectionDispatcher;

impl ProjectionDispatcher for NoopProjectionDispatcher {
    fn keys(&self, _: &str, _: &str, _: &Event) -> Option<Vec<String>> {
        None
    }
    fn project(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &Event,
        _: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }
    fn default_state(&self, _: &str, _: &str) -> Option<String> {
        None
    }
    fn owner_tag_key(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn team_only(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
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
            .expect("failed to build a tokio runtime for command_batcher tests")
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
    // A bigger pool than the bare default (`PgPoolOptions::new()`'s own
    // 10) - deliberately, unlike every other test file's identical
    // provisioning block: this file's own concurrent-submitter tests
    // hold far more connections in flight at once than a typical
    // sequential test ever does (every one of `CONCURRENCY` tasks does
    // its own pre-lock pool reads before joining a batch, plus whichever
    // task is a batch's leader holds its own `tx` for the batch's whole
    // duration), so the bare default queues real work behind an
    // artificially small pool rather than exercising real concurrency.
    let pool_options = db::PgPoolOptions::new().max_connections(50);

    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        let pool = match db::connect_with(&database_url, pool_options).await {
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
    let database_name = "skilj_command_batcher_test";
    if let Err(e) = server.create_database(database_name).await {
        eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
        return None;
    }
    let pool = match db::connect_with(&server.settings().url(database_name), pool_options).await {
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

async fn seed_order_shipped_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "OrderShipped".to_string(),
        schema: r#"{"properties":{"order_id":{"type":"string"}}}"#.to_string(),
        schema_version: 1,
        tag_mappings: vec![TagMapping {
            key: "order".to_string(),
            field: "order_id".to_string(),
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

async fn seed_ship_order_command_type(pool: &Pool, bc: &BoundedContext) -> CommandType {
    let ct = CommandType {
        bounded_context: bc.clone(),
        name: "ShipOrder".to_string(),
        schema: r#"{"properties":{"order_id":{"type":"string"}}}"#.to_string(),
        schema_version: 1,
        tag_mappings: vec![TagMapping {
            key: "order".to_string(),
            field: "order_id".to_string(),
        }],
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        rest_trigger_allowed: true,
    };
    db::upsert_command_type(pool, &ct).await.unwrap();
    ct
}

/// One caller's own end-to-end submission through a shared `CommandBatcher` -
/// derives tags, reads `bounded_context_events` fresh from Postgres,
/// dispatches optimistically, then joins the batch. Deliberately
/// re-derives everything from scratch per call (no cached `EventCache`
/// warm state assumed) so concurrent callers really do race each other
/// through the *whole* optimistic-then-batched path, not just the
/// batcher's own queue.
async fn submit(
    pool: &Pool,
    batcher: &CommandBatcher,
    dispatcher: &ShipOrderDispatcher,
    broadcaster: &EventBroadcaster,
    event_cache: &EventCache,
    command_type: &CommandType,
    payload: &str,
) -> skilj_core::error::Result<SubmitCommandOutcome> {
    let consistency_tags =
        skilj_core::event_store::derive_tags(&command_type.tag_mappings, payload);
    let bounded_context_events = db::list_events_for_bounded_context_matching_tags(
        pool,
        &command_type.bounded_context.name,
        &consistency_tags,
        None,
    )
    .await
    .unwrap();
    let (_boundary, matching_events) =
        skilj_core::event_store::consistency_boundary_and_matching_events(
            &bounded_context_events,
            &consistency_tags,
        );
    let decision = dispatcher
        .dispatch(
            &command_type.bounded_context.name,
            &command_type.name,
            payload,
            &matching_events,
        )
        .unwrap()
        .unwrap();

    batcher
        .submit(
            pool,
            dispatcher,
            &NoopProjectionDispatcher,
            broadcaster,
            event_cache,
            command_type,
            payload,
            "client-1",
            None,
            None,
            &bounded_context_events,
            &consistency_tags,
            &matching_events,
            decision,
            None,
            test_now(),
            None,
            None,
        )
        .await
}

#[test]
fn concurrent_submits_for_the_same_order_accept_exactly_one_and_reject_the_rest() {
    // The real race `CommandBatcher` exists to batch through one lock
    // acquisition instead of many: N real concurrent tasks, all
    // submitting `ShipOrder` for the *same* order at once. Correctness
    // (exactly one `Accepted`, every other one a genuine `Rejected` "already
    // shipped" - never two accepted, never a decider crash, never a
    // dropped request) has to hold regardless of how many of them a
    // single batch happened to coalesce - `submit_command_batch`'s own
    // `extra_committed_events` mechanism (proved deterministically in
    // `submit_command.rs`) is what makes that true even when several of
    // these land in the very same batch.
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_ship_order_command_type(&pool, &bc).await;

        let batcher = CommandBatcher::new();
        let dispatcher = Arc::new(ShipOrderDispatcher);
        let broadcaster = Arc::new(EventBroadcaster::new(64));
        let event_cache = Arc::new(EventCache::new(1000));

        const CONCURRENCY: usize = 25;
        let mut handles = Vec::with_capacity(CONCURRENCY);
        for _ in 0..CONCURRENCY {
            let pool = pool.clone();
            let batcher = batcher.clone();
            let dispatcher = dispatcher.clone();
            let broadcaster = broadcaster.clone();
            let event_cache = event_cache.clone();
            let ct = ct.clone();
            handles.push(tokio::spawn(async move {
                submit(
                    &pool,
                    &batcher,
                    &dispatcher,
                    &broadcaster,
                    &event_cache,
                    &ct,
                    r#"{"order_id":"A"}"#,
                )
                .await
            }));
        }

        let mut accepted = 0;
        let mut rejected = 0;
        for handle in handles {
            match handle.await.unwrap().unwrap() {
                SubmitCommandOutcome::Accepted { .. } => accepted += 1,
                SubmitCommandOutcome::Rejected { reason, .. } => {
                    assert_eq!(reason, "already shipped");
                    rejected += 1;
                }
                other => panic!("ShipOrder never produces {other:?}"),
            }
        }
        assert_eq!(accepted, 1);
        assert_eq!(rejected, CONCURRENCY - 1);

        // Exactly one event ever landed, for real - not just that the
        // outcomes reported it that way.
        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
    });
}

#[test]
fn concurrent_submits_for_different_orders_all_accept_with_gapless_distinct_sequences() {
    // The happy-path concurrency case: no two of these ever conflict
    // (different orders, disjoint consistency tags), so every one must
    // still be accepted - batching commands together must never turn an
    // otherwise-independent command into a spurious conflict.
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_ship_order_command_type(&pool, &bc).await;

        let batcher = CommandBatcher::new();
        let dispatcher = Arc::new(ShipOrderDispatcher);
        let broadcaster = Arc::new(EventBroadcaster::new(64));
        let event_cache = Arc::new(EventCache::new(1000));

        const CONCURRENCY: usize = 25;
        let mut handles = Vec::with_capacity(CONCURRENCY);
        for i in 0..CONCURRENCY {
            let pool = pool.clone();
            let batcher = batcher.clone();
            let dispatcher = dispatcher.clone();
            let broadcaster = broadcaster.clone();
            let event_cache = event_cache.clone();
            let ct = ct.clone();
            handles.push(tokio::spawn(async move {
                submit(
                    &pool,
                    &batcher,
                    &dispatcher,
                    &broadcaster,
                    &event_cache,
                    &ct,
                    &format!(r#"{{"order_id":"order-{i}"}}"#),
                )
                .await
            }));
        }

        let mut sequences: Vec<i64> = Vec::with_capacity(CONCURRENCY);
        for handle in handles {
            match handle.await.unwrap().unwrap() {
                SubmitCommandOutcome::Accepted { events, .. } => {
                    assert_eq!(events.len(), 1);
                    sequences.push(events[0].sequence);
                }
                other => panic!("every distinct order must be accepted, got {other:?}"),
            }
        }

        sequences.sort_unstable();
        let expected: Vec<i64> = (0..CONCURRENCY as i64).collect();
        assert_eq!(
            sequences, expected,
            "every accepted event's own sequence must be distinct and gapless, \
             regardless of how many landed in the same batch"
        );

        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(events.len(), CONCURRENCY);
    });
}

#[test]
fn a_batch_leader_stuck_past_the_idle_in_transaction_timeout_is_killed_and_releases_the_lock() {
    // Codeberg issue #36's own recommendation #3, proved end to end rather
    // than just read from the code: a batch leader's transaction that
    // never sends another statement after acquiring the bounded-context
    // lock - for whatever reason, a genuine hang included - must not wedge
    // every other writer to that bounded context forever. This test
    // fakes the "leader never comes back" half directly (a real hang is
    // hard to manufacture deterministically) via
    // `db::begin_command_batch_leader_tx` on its own, held idle well past
    // a short configured timeout, then proves a *separate*, real
    // `CommandBatcher` submission to the very same bounded context still
    // completes - which it only can if Postgres actually terminated the
    // stuck session and released its `FOR UPDATE` lock on its own, since
    // nothing in this test ever explicitly commits or rolls back the
    // stuck transaction before that second submission starts.
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_ship_order_command_type(&pool, &bc).await;

        let stuck_leader_tx = db::begin_command_batch_leader_tx(
            &pool,
            &bc.name,
            Some(std::time::Duration::from_millis(200)),
        )
        .await
        .expect("acquiring the lock for the stuck leader must succeed");

        // Long enough that Postgres's own `idle_in_transaction_session_timeout`
        // (200ms, set above) has certainly already fired and killed this
        // session - nothing here sends it another statement in the
        // meantime, exactly the "leader never comes back" failure mode
        // Codeberg issue #36 describes.
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

        let batcher = CommandBatcher::new();
        let dispatcher = ShipOrderDispatcher;
        let broadcaster = EventBroadcaster::new(64);
        let event_cache = EventCache::new(1000);

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            submit(
                &pool,
                &batcher,
                &dispatcher,
                &broadcaster,
                &event_cache,
                &ct,
                r#"{"order_id":"A"}"#,
            ),
        )
        .await
        .expect(
            "a fresh submission to the same bounded context must not hang behind a killed \
             leader's stale lock - Postgres terminating the idle session must have released it",
        )
        .unwrap();

        assert!(
            matches!(outcome, SubmitCommandOutcome::Accepted { .. }),
            "expected the fresh submission to be accepted, got {outcome:?}"
        );

        // The stuck leader's own transaction was never committed (Postgres
        // killed the session before that could happen) - dropping it here
        // is just cleanup, not a correctness assertion.
        drop(stuck_leader_tx);
    });
}
