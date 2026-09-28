//! Codeberg issue #21 - proves `db::catch_up_cross_context_route`'s own
//! retry-then-park mechanism for real: a target command submission that
//! fails repeatedly no longer blocks the route's cursor forever (the gap
//! the issue names). Same "test the layer in isolation" shape
//! `skilj-core/tests/submit_command.rs` already uses for
//! `db::decide_and_submit_command` - a hand-rolled `CommandDispatcher`/
//! `CrossContextRouteDispatcher` pair, not the full `skilj` builder
//! facade `skilj/tests/cross_context_route.rs` exercises, since the
//! legitimate typed API gives no way to make a *real* `CrossContextRoute`
//! registration fail deterministically (`route()`'s own return type is
//! tied to `Target::Payload` at compile time, so it can never itself
//! produce a schema mismatch - see this file's own dispatcher for the
//! actual failure injection point instead). Same `DATABASE_URL`-then-
//! embedded-Postgres-then-skip harness as `submit_command.rs` - see its
//! own doc comment for the details, not repeated a third time here.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, ParkedDeliveryKind, Pool};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, CommandType, Event, EventBroadcaster, EventOrigin,
    EventType,
};
use skilj_core::plugin::{
    CommandDispatcher, CrossContextRouteDispatcher, CrossContextRouteInfo,
    CrossContextRouteStartFrom, ProjectionDispatcher, SnapshotDispatcher,
};
use skilj_core::shared::{
    generate_token_id, CommandDecision, EventSpec, Metadata, Tag, TagMapping,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const ROUTE_NAME: &str = "ShipToReserve";

/// Fails `dispatch()` for the first `fail_until` calls (a deterministic
/// stand-in for "the target keeps rejecting/erroring"), then accepts
/// every call after - proof both halves of the mechanism: exhausting
/// `RetryPolicy` parks the occurrence, and a later successful call (the
/// same shape `retryParkedDelivery`'s own redrive makes) still works.
struct FlakyCommandDispatcher {
    calls: AtomicUsize,
    fail_until: usize,
}

impl FlakyCommandDispatcher {
    fn new(fail_until: usize) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            fail_until,
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl CommandDispatcher for FlakyCommandDispatcher {
    fn dispatch(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        payload: &str,
        _matching_events: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call <= self.fail_until {
            return Some(Err(
                skilj_core::event_store::Error::PayloadDoesNotMatchSchema.into(),
            ));
        }
        Some(Ok(CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "StockReserved".to_string(),
                payload: serde_json::from_str(payload).expect("test payload is always JSON"),
            }],
        }))
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
        unreachable!("this file's own fixture never opts into snapshot()")
    }
}

/// Translates `OrderShipped`'s payload straight through unchanged -
/// always `Some(Ok(Some(...)))`, since `route()` itself is never this
/// test's own failure injection point (see this file's own doc comment).
struct PassthroughRouteDispatcher {
    info: CrossContextRouteInfo,
}

impl CrossContextRouteDispatcher for PassthroughRouteDispatcher {
    fn routes(&self) -> Vec<CrossContextRouteInfo> {
        vec![self.info]
    }

    fn route(
        &self,
        route_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<String>, serde_json::Error>> {
        if route_name != self.info.name {
            return None;
        }
        Some(Ok(Some(source_payload_json.to_string())))
    }
}

struct NoopProjectionDispatcher;

impl ProjectionDispatcher for NoopProjectionDispatcher {
    fn keys(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
        _event: &Event,
    ) -> Option<Vec<String>> {
        None
    }
    fn project(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
        _state_json: &str,
        _event: &Event,
        _key: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }
    fn default_state(&self, _bounded_context: &str, _projection_name: &str) -> Option<String> {
        None
    }
    fn owner_tag_key(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
    ) -> Option<Option<&'static str>> {
        None
    }
    fn team_only(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
    ) -> Option<Option<&'static str>> {
        None
    }
}

struct NoopSnapshotDispatcher;

impl SnapshotDispatcher for NoopSnapshotDispatcher {
    fn snapshot_names(&self, _bounded_context: &str) -> Vec<&'static str> {
        Vec::new()
    }
    fn tag_key(&self, _bounded_context: &str, _snapshot_name: &str) -> Option<&'static str> {
        None
    }
    fn owner_tag_key(
        &self,
        _bounded_context: &str,
        _snapshot_name: &str,
    ) -> Option<Option<&'static str>> {
        None
    }
    fn version(&self, _bounded_context: &str, _snapshot_name: &str) -> Option<u64> {
        None
    }
    fn fold(
        &self,
        _bounded_context: &str,
        _snapshot_name: &str,
        _state_json: &str,
        _event: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }
    fn default_state(&self, _bounded_context: &str, _snapshot_name: &str) -> Option<String> {
        None
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
            .expect("failed to build a tokio runtime for cross_context_route_parking tests")
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
        return Some(TestDb { pool });
    }

    let url = skilj_test_support::database_url("skilj_cross_context_route_parking_test").await?;
    let pool = match db::connect(&url).await {
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

async fn seed_stock_reserved_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "StockReserved".to_string(),
        schema: r#"{"properties":{"order_id":{"type":"string"}}}"#.to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: false,
        direct_creation_allowed: false,
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

async fn seed_command_type(pool: &Pool, bc: &BoundedContext, name: &str) -> CommandType {
    let ct = CommandType {
        bounded_context: bc.clone(),
        name: name.to_string(),
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

async fn insert_order_shipped(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    order_id: &str,
) -> i64 {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let e = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: format!(r#"{{"order_id":"{order_id}"}}"#),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: "test-writer".to_string(),
            created_at: test_now(),
            correlation_id: None,
            causation_id: None,
        },
        sequence: seq,
        tags: vec![Tag {
            key: "order".to_string(),
            value: Some(order_id.to_string()),
        }],
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event(pool, &e, None).await.unwrap();
    seq
}

/// A persistently-failing target submission parks after `RetryPolicy`
/// exhausts (rather than blocking the route's cursor forever), the
/// blocked occurrence is genuinely throttled by backoff in between (not
/// re-attempted on every tick), a later manual redrive using the parked
/// row's own stored payload succeeds and the row is deleted (the shape
/// `retryParkedDelivery`'s own resolver uses), and a second occurrence
/// that's discarded without ever being redriven leaves no event behind.
#[test]
fn a_persistently_failing_target_parks_instead_of_blocking_forever() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };

        let shipping_bc = seed_bounded_context(&pool).await;
        let inventory_bc = seed_bounded_context(&pool).await;
        let source_et = seed_order_shipped_event_type(&pool, &shipping_bc).await;
        let target_ct = seed_command_type(&pool, &inventory_bc, "ReserveStock").await;
        seed_stock_reserved_event_type(&pool, &inventory_bc).await;

        let route_info = CrossContextRouteInfo {
            name: ROUTE_NAME,
            source_bounded_context: Box::leak(shipping_bc.name.clone().into_boxed_str()),
            source_event_type: "OrderShipped",
            target_bounded_context: Box::leak(inventory_bc.name.clone().into_boxed_str()),
            target_command_type: "ReserveStock",
            start_from: CrossContextRouteStartFrom::Beginning,
        };
        let route_dispatcher = PassthroughRouteDispatcher { info: route_info };
        // Fails the first 2 calls, accepts from the 3rd - paired with a
        // `max_attempts: 2` policy below, so the occurrence parks right
        // after its 2nd failure without ever reaching a 3rd attempt.
        let command_dispatcher = FlakyCommandDispatcher::new(2);
        let projection_dispatcher = NoopProjectionDispatcher;
        let snapshot_dispatcher = NoopSnapshotDispatcher;
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(30),
            1.0,
            Duration::from_millis(30),
            2,
        );

        insert_order_shipped(&pool, &shipping_bc, &source_et, "order-1").await;

        // Tick 1: dispatch() fails once (call 1/2) - not yet exhausted,
        // so this occurrence is blocked with backoff, not parked.
        db::catch_up_cross_context_route(
            &pool,
            &route_info,
            &route_dispatcher,
            &command_dispatcher,
            &projection_dispatcher,
            &snapshot_dispatcher,
            &broadcaster,
            &event_cache,
            None,
            &retry_policy,
        )
        .await
        .unwrap();
        assert_eq!(command_dispatcher.call_count(), 1);
        assert!(db::get_event_by_sequence(&pool, &shipping_bc.name, 0)
            .await
            .unwrap()
            .is_some());
        assert!(db::list_parked_deliveries(&pool, &inventory_bc.name)
            .await
            .unwrap()
            .is_empty());

        // Tick 2, called immediately (well within the 30ms backoff): the
        // "not yet time to retry" gate must skip this tick entirely -
        // dispatch() must not be called a 2nd time yet.
        db::catch_up_cross_context_route(
            &pool,
            &route_info,
            &route_dispatcher,
            &command_dispatcher,
            &projection_dispatcher,
            &snapshot_dispatcher,
            &broadcaster,
            &event_cache,
            None,
            &retry_policy,
        )
        .await
        .unwrap();
        assert_eq!(
            command_dispatcher.call_count(),
            1,
            "backoff must throttle retries, not re-attempt on every tick"
        );

        tokio::time::sleep(Duration::from_millis(40)).await;

        // Tick 3: backoff has elapsed - dispatch() fails again (call
        // 2/2), which exhausts the policy (max_attempts: 2). The
        // occurrence is parked and the cursor finally advances past it.
        db::catch_up_cross_context_route(
            &pool,
            &route_info,
            &route_dispatcher,
            &command_dispatcher,
            &projection_dispatcher,
            &snapshot_dispatcher,
            &broadcaster,
            &event_cache,
            None,
            &retry_policy,
        )
        .await
        .unwrap();
        assert_eq!(command_dispatcher.call_count(), 2);
        // Proof the cursor actually advanced past the parked occurrence
        // (rather than merely not erroring) comes later: the "second
        // occurrence" section below posts a *new* event and shows the
        // route reacts to it on its own next tick - if the cursor were
        // still stuck on order-1, that would never happen.

        let parked = db::list_parked_deliveries(&pool, &inventory_bc.name)
            .await
            .unwrap();
        assert_eq!(parked.len(), 1);
        let parked_delivery = &parked[0];
        assert_eq!(parked_delivery.kind, ParkedDeliveryKind::CrossContextRoute);
        assert_eq!(
            parked_delivery.source,
            format!("cross-context-route:{ROUTE_NAME}")
        );
        assert_eq!(parked_delivery.identifier, "0");
        assert_eq!(
            parked_delivery.target_bounded_context.as_deref(),
            Some(inventory_bc.name.as_str())
        );
        assert_eq!(
            parked_delivery.target_command_type.as_deref(),
            Some("ReserveStock")
        );
        assert_eq!(parked_delivery.attempt_count, 2);
        assert_eq!(
            parked_delivery.request_json,
            serde_json::json!({"order_id": "order-1"})
        );
        assert!(!parked_delivery.error.is_empty());

        // A failed manual retry (the shape `retryParkedDelivery` takes
        // when redrive itself errors again) bumps attempt_count/error/
        // last_failed_at rather than deleting the row.
        db::record_parked_delivery_retry_failure(
            &pool,
            &inventory_bc.name,
            &parked_delivery.id,
            "still down",
            Utc::now(),
        )
        .await
        .unwrap();
        let after_failed_retry =
            db::get_parked_delivery(&pool, &inventory_bc.name, &parked_delivery.id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(after_failed_retry.attempt_count, 3);
        assert_eq!(after_failed_retry.error, "still down");

        // A successful manual retry - the exact redrive
        // `retryParkedDelivery`'s own resolver makes: resubmit the
        // parked row's own stored payload through `decide_and_submit_command`
        // under `parked_delivery_redrive_identity` (the dispatcher now
        // accepts, call 3 is past `fail_until`). On success the row is
        // deleted.
        let payload = serde_json::to_string(&after_failed_retry.request_json).unwrap();
        let (client_id, idempotency_key) = db::parked_delivery_redrive_identity(
            &after_failed_retry,
            db::CROSS_CONTEXT_ROUTE_CLIENT_ID,
            None,
        )
        .unwrap();
        assert_eq!(client_id, db::CROSS_CONTEXT_ROUTE_CLIENT_ID);
        let redrive = || {
            db::decide_and_submit_command(
                &pool,
                &command_dispatcher,
                &projection_dispatcher,
                &snapshot_dispatcher,
                &broadcaster,
                &event_cache,
                &target_ct,
                &payload,
                &client_id,
                None,
                None,
                None,
                Utc::now(),
                Some(&idempotency_key),
            )
        };
        assert!(matches!(
            redrive().await.unwrap(),
            db::SubmitCommandOutcome::Accepted { .. }
        ));
        // Redriving again - a retry whose submission committed but whose
        // row delete then failed - lands nothing new: it runs under the
        // same key the route itself used.
        assert!(matches!(
            redrive().await.unwrap(),
            db::SubmitCommandOutcome::Deduplicated { .. }
        ));
        let deleted = db::delete_parked_delivery(&pool, &inventory_bc.name, &parked_delivery.id)
            .await
            .unwrap();
        assert!(deleted.is_some());
        assert!(db::list_parked_deliveries(&pool, &inventory_bc.name)
            .await
            .unwrap()
            .is_empty());
        let routed_command = db::list_commands_for_bounded_context(&pool, &inventory_bc.name)
            .await
            .unwrap();
        assert_eq!(routed_command.len(), 1);
        assert_eq!(routed_command[0].command_type.name, "ReserveStock");

        // A second occurrence, discarded without ever being redriven -
        // proof discard alone (no retry) also just removes the row,
        // leaving no command/event behind.
        insert_order_shipped(&pool, &shipping_bc, &source_et, "order-2").await;
        let flaky_again = FlakyCommandDispatcher::new(usize::MAX);
        db::catch_up_cross_context_route(
            &pool,
            &route_info,
            &route_dispatcher,
            &flaky_again,
            &projection_dispatcher,
            &snapshot_dispatcher,
            &broadcaster,
            &event_cache,
            None,
            &retry_policy,
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        db::catch_up_cross_context_route(
            &pool,
            &route_info,
            &route_dispatcher,
            &flaky_again,
            &projection_dispatcher,
            &snapshot_dispatcher,
            &broadcaster,
            &event_cache,
            None,
            &retry_policy,
        )
        .await
        .unwrap();
        let parked_again = db::list_parked_deliveries(&pool, &inventory_bc.name)
            .await
            .unwrap();
        assert_eq!(parked_again.len(), 1);
        assert_eq!(parked_again[0].identifier, "1");

        let discarded = db::delete_parked_delivery(&pool, &inventory_bc.name, &parked_again[0].id)
            .await
            .unwrap();
        assert!(discarded.is_some());
        assert!(db::list_parked_deliveries(&pool, &inventory_bc.name)
            .await
            .unwrap()
            .is_empty());
        // Still exactly the one command from the successful retry above -
        // the discarded occurrence never submitted anything.
        let commands = db::list_commands_for_bounded_context(&pool, &inventory_bc.name)
            .await
            .unwrap();
        assert_eq!(commands.len(), 1);
    });
}

/// Codeberg issue #25 review (docs/architecture.md §56): two concurrent
/// catch-up ticks for the identical route/occurrence must never both
/// park it - `catch_up_cross_context_route`'s own advisory lock should
/// serialize them so only the first ever gets far enough to see the
/// retry policy as exhausted; the second, once it acquires the lock,
/// finds the cursor already advanced past the occurrence and has
/// nothing left to do. Genuinely concurrent via `tokio::join!`, not
/// sequenced with a sleep - the same "prove the race is closed, don't
/// assume a lucky interleaving" shape
/// `a_persistently_failing_target_parks_instead_of_blocking_forever`'s
/// own file already uses for the sequential half of this mechanism.
#[test]
fn concurrent_catch_up_ticks_never_park_the_same_occurrence_twice() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };

        let shipping_bc = seed_bounded_context(&pool).await;
        let inventory_bc = seed_bounded_context(&pool).await;
        let source_et = seed_order_shipped_event_type(&pool, &shipping_bc).await;
        seed_command_type(&pool, &inventory_bc, "ReserveStock").await;
        seed_stock_reserved_event_type(&pool, &inventory_bc).await;

        let route_info = CrossContextRouteInfo {
            name: ROUTE_NAME,
            source_bounded_context: Box::leak(shipping_bc.name.clone().into_boxed_str()),
            source_event_type: "OrderShipped",
            target_bounded_context: Box::leak(inventory_bc.name.clone().into_boxed_str()),
            target_command_type: "ReserveStock",
            start_from: CrossContextRouteStartFrom::Beginning,
        };
        let route_dispatcher = PassthroughRouteDispatcher { info: route_info };
        // Always fails - paired with `max_attempts: 1` below, a single
        // failed attempt already exhausts the policy and parks, the
        // narrowest possible window for the race this test targets.
        let command_dispatcher = FlakyCommandDispatcher::new(usize::MAX);
        let projection_dispatcher = NoopProjectionDispatcher;
        let snapshot_dispatcher = NoopSnapshotDispatcher;
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(30),
            1.0,
            Duration::from_millis(30),
            1,
        );

        insert_order_shipped(&pool, &shipping_bc, &source_et, "order-race").await;

        // Two ticks for the identical route/occurrence, run concurrently
        // rather than sequenced - without `catch_up_cross_context_route`'s
        // own advisory lock, both would independently read the same
        // not-yet-exhausted retry state, both fail once, both compute
        // the policy as exhausted, and both call `insert_parked_delivery`
        // for the identical occurrence.
        let tick = || {
            db::catch_up_cross_context_route(
                &pool,
                &route_info,
                &route_dispatcher,
                &command_dispatcher,
                &projection_dispatcher,
                &snapshot_dispatcher,
                &broadcaster,
                &event_cache,
                None,
                &retry_policy,
            )
        };
        let (result_a, result_b) = tokio::join!(tick(), tick());
        result_a.unwrap();
        result_b.unwrap();

        let parked = db::list_parked_deliveries(&pool, &inventory_bc.name)
            .await
            .unwrap();
        assert_eq!(
            parked.len(),
            1,
            "two concurrent catch-up ticks raced to park the identical occurrence twice - \
             the advisory lock serializing catch_up_cross_context_route is not closing the race"
        );
    });
}

/// `migrate_parked_deliveries_dedup_and_unique_index` converging a schema
/// migrated under the earlier `(source, kind, identifier)` index onto the
/// token-scoped one: the legacy index is dropped, the new one is in place
/// (so a second token's report of the same occurrence gets its own row,
/// while `CrossContextRoute`'s own `NULL`-token rows still upsert onto
/// each other), and re-running it is a no-op.
#[test]
fn legacy_parked_delivery_index_migrates_to_the_token_scoped_one() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let schema = format!("\"bc_{}\"", bc.name);
        // Recreate the pre-migration state: only the legacy index.
        for ddl in [
            format!("DROP INDEX {schema}.parked_deliveries_occurrence_key"),
            format!(
                "CREATE UNIQUE INDEX parked_deliveries_source_kind_identifier_key \
                 ON {schema}.parked_deliveries (source, kind, identifier)"
            ),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(ddl))
                .execute(&pool)
                .await
                .unwrap();
        }

        for _ in 0..2 {
            db::migrate_parked_deliveries_dedup_and_unique_index(&pool, &bc.name)
                .await
                .unwrap();
        }
        let indexes: Vec<String> = sqlx::query_scalar(
            "SELECT indexname::text FROM pg_indexes \
             WHERE schemaname = $1 AND tablename = 'parked_deliveries' ORDER BY 1",
        )
        .bind(format!("bc_{}", bc.name))
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            indexes,
            vec![
                "parked_deliveries_occurrence_key",
                // docs/architecture.md §86 - from provisioning, untouched
                // by this migration.
                "parked_deliveries_page_order",
                "parked_deliveries_pkey"
            ]
        );

        let park = |kind, token: Option<&'static str>| {
            let pool = pool.clone();
            let bc_name = bc.name.clone();
            async move {
                db::insert_parked_delivery(
                    &pool,
                    &bc_name,
                    "kafka-inbound",
                    kind,
                    "orders:0:1",
                    token,
                    None,
                    None,
                    &serde_json::json!({}),
                    "boom",
                    1,
                    test_now(),
                    test_now(),
                )
                .await
                .unwrap()
            }
        };
        let a = park(ParkedDeliveryKind::ExternalEvent, Some("token-a")).await;
        let a_again = park(ParkedDeliveryKind::ExternalEvent, Some("token-a")).await;
        let b = park(ParkedDeliveryKind::ExternalEvent, Some("token-b")).await;
        let route = park(ParkedDeliveryKind::CrossContextRoute, None).await;
        let route_again = park(ParkedDeliveryKind::CrossContextRoute, None).await;
        assert_eq!(a.id, a_again.id);
        assert_ne!(a.id, b.id);
        assert_eq!(route.id, route_again.id);
        assert_eq!(
            db::list_parked_deliveries(&pool, &bc.name)
                .await
                .unwrap()
                .len(),
            3
        );
    });
}

/// One catch-up tick loads at most `MAX_EVENTS_PER_CATCH_UP_TICK` source
/// events, so a new route over a long backlog never loads it whole; the
/// next tick continues from the persisted cursor. 1003 events against a
/// 1000-event cache window: the first tick misses the cache (the
/// `LIMIT`ed Postgres path), the second hits it.
#[test]
fn a_catch_up_tick_is_bounded_and_the_next_one_continues() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let shipping_bc = seed_bounded_context(&pool).await;
        let inventory_bc = seed_bounded_context(&pool).await;
        let source_et = seed_order_shipped_event_type(&pool, &shipping_bc).await;
        seed_command_type(&pool, &inventory_bc, "ReserveStock").await;
        seed_stock_reserved_event_type(&pool, &inventory_bc).await;
        let route_info = CrossContextRouteInfo {
            name: ROUTE_NAME,
            source_bounded_context: Box::leak(shipping_bc.name.clone().into_boxed_str()),
            source_event_type: "OrderShipped",
            target_bounded_context: Box::leak(inventory_bc.name.clone().into_boxed_str()),
            target_command_type: "ReserveStock",
            start_from: CrossContextRouteStartFrom::Beginning,
        };
        let route_dispatcher = PassthroughRouteDispatcher { info: route_info };
        let command_dispatcher = FlakyCommandDispatcher::new(0);
        let event_cache = EventCache::new(1000);
        let cap = db::MAX_EVENTS_PER_CATCH_UP_TICK as usize;
        for i in 0..cap + 3 {
            insert_order_shipped(&pool, &shipping_bc, &source_et, &format!("order-{i}")).await;
        }

        let (projection_dispatcher, snapshot_dispatcher) =
            (NoopProjectionDispatcher, NoopSnapshotDispatcher);
        let broadcaster = EventBroadcaster::new(16);
        let retry_policy = skilj_retry::RetryPolicy::default();
        let tick = || {
            db::catch_up_cross_context_route(
                &pool,
                &route_info,
                &route_dispatcher,
                &command_dispatcher,
                &projection_dispatcher,
                &snapshot_dispatcher,
                &broadcaster,
                &event_cache,
                None,
                &retry_policy,
            )
        };
        tick().await.unwrap();
        assert_eq!(command_dispatcher.call_count(), cap);
        tick().await.unwrap();
        assert_eq!(command_dispatcher.call_count(), cap + 3);
        tick().await.unwrap();
        assert_eq!(
            command_dispatcher.call_count(),
            cap + 3,
            "nothing left to route"
        );
    });
}
