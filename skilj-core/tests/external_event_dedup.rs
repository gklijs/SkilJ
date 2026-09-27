//! Tests for `db::create_and_insert_external_event`'s own `dedupe`
//! parameter ([docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event), specs/skilj.allium's own `rule
//! CreateExternalEvent` - the `dedupe_partition_key?`/`dedupe_sequence?`
//! pair on `SubmitExternalEvent`). `event_creation_surfaces.rs` already
//! covers the pure `event_store::create_external_event` function (which
//! this dedup mechanism never touches - it lives entirely in the `db`
//! layer, exactly like `submit_command`'s own idempotency-key check);
//! this file is the real-Postgres counterpart, the same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj-core/tests/submit_command.rs` - see its own doc comment for
//! the details, not repeated a third time here.

use chrono::Utc;
use skilj_core::access_control::{ExternalEventToken, TokenStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, CreateExternalEventOutcome, DedupeCursor, Pool};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventBroadcaster, EventType,
};
use skilj_core::plugin::ProjectionDispatcher;
use skilj_core::shared::{generate_token_id, TagMapping};

/// Nothing in this file registers a projection - every method reports
/// "pair isn't registered at all", the same convention every other
/// `None` return in this trait already uses.
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

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for external_event_dedup tests")
    })
}

async fn test_pool() -> Option<Pool> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.pool.clone())
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
        eprintln!("skipping: {label} migration failed: {e}");
        return None;
    }
    Some(pool)
}

async fn provision() -> Option<TestDb> {
    let database_url = if let Ok(database_url) = std::env::var("DATABASE_URL") {
        database_url
    } else {
        let url = skilj_test_support::database_url("skilj_external_event_dedup_test").await?;
        let pool = connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb { pool });
    };

    let pool = connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb { pool })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    use chrono::SubsecRound;
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

async fn seed_order_placed_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "OrderPlaced".to_string(),
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

fn adapter(event_type: EventType, id: &str) -> ExternalEventToken {
    ExternalEventToken {
        id: id.to_string(),
        secret: "s3cr3t".to_string(),
        status: TokenStatus::Active,
        created_at: test_now(),
        revoked_at: None,
        event_type,
        scope: None,
    }
}

async fn create(
    pool: &Pool,
    broadcaster: &EventBroadcaster,
    event_cache: &EventCache,
    token: &ExternalEventToken,
    order_id: &str,
    dedupe: Option<DedupeCursor<'_>>,
) -> CreateExternalEventOutcome {
    db::create_and_insert_external_event(
        pool,
        &NoopProjectionDispatcher,
        broadcaster,
        event_cache,
        token,
        format!(r#"{{"order_id":"{order_id}"}}"#),
        "kafka".to_string(),
        None,
        None,
        None,
        dedupe,
        test_now(),
        None,
    )
    .await
    .unwrap()
}

/// [docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event): a submission carrying neither
/// `dedupe_partition_key` nor `dedupe_sequence` behaves exactly as every
/// submission did before this mechanism existed - no lookup, no write to
/// `external_message_cursors`, an event created every time, even for the
/// exact same payload submitted twice in a row.
/// (`ExternalEventIngestion.OmittingTheDedupePairChangesNothing`.)
#[test]
fn omitting_dedupe_creates_an_event_every_time() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_order_placed_event_type(&pool, &bc).await;
        let token = adapter(et, "adapter-1");
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let first = create(&pool, &broadcaster, &event_cache, &token, "A", None).await;
        let second = create(&pool, &broadcaster, &event_cache, &token, "A", None).await;

        assert!(matches!(first, CreateExternalEventOutcome::Created(_)));
        assert!(matches!(second, CreateExternalEventOutcome::Created(_)));

        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            2,
            "no dedupe means no dedupe, even for an identical payload"
        );
    });
}

/// [docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event): a genuinely new, higher `dedupe_sequence`
/// for a `(adapter, partition_key)` pair is created exactly as it always
/// was, and becomes the new watermark.
#[test]
fn a_new_higher_sequence_is_created_and_advances_the_watermark() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_order_placed_event_type(&pool, &bc).await;
        let token = adapter(et, "adapter-1");
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let first = create(
            &pool,
            &broadcaster,
            &event_cache,
            &token,
            "A",
            Some(DedupeCursor {
                partition_key: "orders-topic:0",
                sequence: 10,
            }),
        )
        .await;
        let second = create(
            &pool,
            &broadcaster,
            &event_cache,
            &token,
            "B",
            Some(DedupeCursor {
                partition_key: "orders-topic:0",
                sequence: 11,
            }),
        )
        .await;

        assert!(matches!(first, CreateExternalEventOutcome::Created(_)));
        assert!(matches!(second, CreateExternalEventOutcome::Created(_)));
        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
    });
}

/// The central case: a redelivered message (same or lower
/// `dedupe_sequence` for the same `(adapter, partition_key)`) creates no
/// event at all - `CreateExternalEventOutcome::Redelivered`, nothing
/// written. Proves both "same sequence again" (the exact retry case) and
/// "lower sequence" (an out-of-order-but-already-superseded redelivery)
/// are both recognised. (`ARedeliveryProducesNoEventAndNoOutcome`.)
#[test]
fn a_redelivered_or_stale_sequence_creates_no_event() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_order_placed_event_type(&pool, &bc).await;
        let token = adapter(et, "adapter-1");
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let first = create(
            &pool,
            &broadcaster,
            &event_cache,
            &token,
            "A",
            Some(DedupeCursor {
                partition_key: "orders-topic:0",
                sequence: 10,
            }),
        )
        .await;
        assert!(matches!(first, CreateExternalEventOutcome::Created(_)));

        // Exact redelivery of the same message.
        let redelivered = create(
            &pool,
            &broadcaster,
            &event_cache,
            &token,
            "A",
            Some(DedupeCursor {
                partition_key: "orders-topic:0",
                sequence: 10,
            }),
        )
        .await;
        assert!(matches!(
            redelivered,
            CreateExternalEventOutcome::Redelivered
        ));

        // A stale replay from further back in the partition.
        let stale = create(
            &pool,
            &broadcaster,
            &event_cache,
            &token,
            "C",
            Some(DedupeCursor {
                partition_key: "orders-topic:0",
                sequence: 3,
            }),
        )
        .await;
        assert!(matches!(stale, CreateExternalEventOutcome::Redelivered));

        // Only the one, real, non-redelivered event actually exists.
        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "a redelivery/stale replay must write nothing"
        );
    });
}

/// [docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event): scoped to `(adapter, partition_key)` alone,
/// not per event type - two different adapters for the *same* event type
/// independently choosing the identical partition key string never
/// deduplicate each other's messages, exactly the cross-tenant collision
/// class [docs/architecture.md §37](../../docs/architecture.md#idempotency-keys-client-id-scoping) closed for `idempotency_keys`.
#[test]
fn two_different_adapters_sharing_a_partition_key_string_do_not_collide() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_order_placed_event_type(&pool, &bc).await;
        let adapter_a = adapter(et.clone(), "adapter-a");
        let adapter_b = adapter(et, "adapter-b");
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let a = create(
            &pool,
            &broadcaster,
            &event_cache,
            &adapter_a,
            "A",
            Some(DedupeCursor {
                partition_key: "0",
                sequence: 5,
            }),
        )
        .await;
        // Same literal partition_key string, a completely different
        // adapter, a lower sequence than adapter_a's own watermark -
        // must still be created for real, not swallowed as if it were
        // adapter_a's own redelivery.
        let b = create(
            &pool,
            &broadcaster,
            &event_cache,
            &adapter_b,
            "B",
            Some(DedupeCursor {
                partition_key: "0",
                sequence: 1,
            }),
        )
        .await;

        assert!(matches!(a, CreateExternalEventOutcome::Created(_)));
        assert!(
            matches!(b, CreateExternalEventOutcome::Created(_)),
            "adapter_b's own lower sequence must not collide with adapter_a's watermark, \
             got {b:?}"
        );
        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
    });
}

/// [docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event): a different `partition_key` from the *same*
/// adapter has its own, independent watermark - a low sequence on a
/// fresh partition is not mistaken for a stale replay of a different,
/// already-advanced partition.
#[test]
fn different_partitions_from_the_same_adapter_have_independent_watermarks() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_order_placed_event_type(&pool, &bc).await;
        let token = adapter(et, "adapter-1");
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let partition_0 = create(
            &pool,
            &broadcaster,
            &event_cache,
            &token,
            "A",
            Some(DedupeCursor {
                partition_key: "orders-topic:0",
                sequence: 100,
            }),
        )
        .await;
        let partition_1 = create(
            &pool,
            &broadcaster,
            &event_cache,
            &token,
            "B",
            Some(DedupeCursor {
                partition_key: "orders-topic:1",
                sequence: 1,
            }),
        )
        .await;

        assert!(matches!(
            partition_0,
            CreateExternalEventOutcome::Created(_)
        ));
        assert!(
            matches!(partition_1, CreateExternalEventOutcome::Created(_)),
            "partition 1's own low sequence must not be judged against partition 0's \
             watermark, got {partition_1:?}"
        );
    });
}
