//! Tests for `event_cache::EventCache` - the in-memory per-bounded-context
//! event cache the spec describes (drift audit finding #8, closed
//! 2026-08-19 - see project memory `skilj-drift-audit-2026-08-18`). Real
//! Postgres throughout, unlike most of this crate's other pure-function
//! tests - `EventCache` itself is an I/O-touching type (the freshness
//! check needs a real `db::latest_sequence` read), so there's no pure
//! half to test in isolation. Same `DATABASE_URL`-then-embedded-Postgres-
//! then-skip harness as `skilj-core/tests/submit_command.rs` - see its
//! own doc comment for the details, not repeated a third time here;
//! `seed_bounded_context`/`seed_order_shipped_event_type`'s own shape is
//! borrowed from that file too.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::shared::{generate_token_id, Metadata};

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
            .expect("failed to build a tokio runtime for event_cache tests")
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
    let database_name = "skilj_event_cache_test";
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

async fn seed_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "OrderShipped".to_string(),
        schema: r#"{"properties":{}}"#.to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
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

/// Inserts one real, committed row directly - bypassing `EventCache`
/// entirely (no `append` call), the same "simulates a writer this
/// process's own cache never heard about" role
/// `submit_command.rs::insert_concurrent_event` already plays for DCB
/// conflict tests.
async fn insert_event_bypassing_cache(pool: &Pool, bc: &BoundedContext, et: &EventType) -> Event {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let e = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: "{}".to_string(),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: "bypassing-writer".to_string(),
            created_at: test_now(),
        },
        sequence: seq,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event(pool, &e, None).await.unwrap();
    e
}

#[test]
fn warm_seeds_the_most_recent_capacity_events_and_covers_from_there() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let mut events = Vec::new();
        for _ in 0..5 {
            events.push(insert_event_bypassing_cache(&pool, &bc, &et).await);
        }

        let cache = EventCache::new(3);
        cache.warm(&pool, &bc.name).await.unwrap();

        // Only the 3 most recent (sequences 2,3,4) are covered - asking
        // for anything from further back than that is a genuine coverage
        // miss, not a wrong answer.
        assert_eq!(
            cache.try_events_after(&pool, &bc.name, -1).await.unwrap(),
            None
        );
        assert_eq!(
            cache
                .try_events_after(&pool, &bc.name, events[0].sequence)
                .await
                .unwrap(),
            None
        );

        // Exactly at the covered boundary (the instant just before the
        // oldest cached event) and anything after it are both answerable.
        let from_boundary = cache
            .try_events_after(&pool, &bc.name, events[1].sequence)
            .await
            .unwrap()
            .expect("after_sequence at the window's own boundary must be covered");
        assert_eq!(
            from_boundary.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![events[2].sequence, events[3].sequence, events[4].sequence]
        );
    });
}

#[test]
fn append_evicts_the_oldest_event_once_over_capacity() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;

        let cache = EventCache::new(2);
        cache.warm(&pool, &bc.name).await.unwrap(); // empty at this point

        let mut events = Vec::new();
        for _ in 0..3 {
            let e = insert_event_bypassing_cache(&pool, &bc, &et).await;
            cache.append(&e).await;
            events.push(e);
        }

        // The very first event is now evicted - a full-history request
        // is a coverage miss...
        assert_eq!(
            cache.try_events_after(&pool, &bc.name, -1).await.unwrap(),
            None
        );
        // ...but the 2 most recent are still directly answerable, with no
        // Postgres catch-up needed (nothing was written outside `append`
        // itself, so a stale read here would be a real bug, not a
        // freshness gap).
        let recent = cache
            .try_events_after(&pool, &bc.name, events[0].sequence)
            .await
            .unwrap()
            .expect("the 2 most recent events must still be covered");
        assert_eq!(
            recent.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![events[1].sequence, events[2].sequence]
        );
    });
}

/// The one test that actually proves multi-instance freshness (the
/// user's own explicit requirement, confirmed via `AskUserQuestion`
/// before this pass began): two independent `EventCache`s, standing in
/// for two separate process instances against the same shared database -
/// neither ever calls the other's `append`, so the only way the second
/// one can see what the first wrote is the freshness check inside
/// `try_events_after` itself.
#[test]
fn a_second_independent_cache_sees_a_write_it_never_appended_itself() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;

        let instance_a = EventCache::new(1000);
        let instance_b = EventCache::new(1000);
        instance_a.warm(&pool, &bc.name).await.unwrap();
        instance_b.warm(&pool, &bc.name).await.unwrap();
        // Both start out agreeing there's nothing yet.
        assert_eq!(
            instance_a
                .try_events_after(&pool, &bc.name, -1)
                .await
                .unwrap(),
            Some(Vec::new())
        );

        // A real commit, through instance A's own cache alone -
        // `insert_event_bypassing_cache` plus a manual `append` on `a`
        // only, exactly mirroring what `db::create_and_insert_direct_event`
        // does for real (insert, then append to the one cache the
        // committing instance holds).
        let event = insert_event_bypassing_cache(&pool, &bc, &et).await;
        instance_a.append(&event).await;

        // Instance B never saw that `append` call - if it answered from
        // its own stale, empty local state, this would come back
        // `Some(vec![])` instead, silently violating
        // LatestEventsAlwaysAvailable. It must self-heal via Postgres.
        let seen_by_b = instance_b
            .try_events_after(&pool, &bc.name, -1)
            .await
            .unwrap()
            .expect("instance B must still be able to prove coverage from the beginning");
        assert_eq!(seen_by_b.len(), 1);
        assert_eq!(seen_by_b[0].sequence, event.sequence);
    });
}

#[test]
fn try_event_by_sequence_finds_a_cached_event_and_misses_a_not_yet_seen_one() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let event = insert_event_bypassing_cache(&pool, &bc, &et).await;

        let cache = EventCache::new(1000);
        cache.warm(&pool, &bc.name).await.unwrap();

        let found = cache
            .try_event_by_sequence(&pool, &bc.name, event.sequence)
            .await
            .unwrap();
        assert_eq!(found, Some(event.clone()));

        // A sequence this bounded context never had at all - correctly a
        // miss (not an error), falling back to `db::get_event_by_sequence`
        // is the caller's own job (`get_event_by_sequence_cached`).
        let missing = cache
            .try_event_by_sequence(&pool, &bc.name, event.sequence + 1000)
            .await
            .unwrap();
        assert_eq!(missing, None);
    });
}
