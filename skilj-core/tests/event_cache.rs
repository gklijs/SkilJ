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
use skilj_core::shared::{generate_token_id, Metadata, PrivateField, PrivateFieldKind, Tag};

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    pool: Pool,
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
    let url = skilj_test_support::database_url("skilj_event_cache_test").await?;
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

async fn seed_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "OrderShipped".to_string(),
        schema: r#"{"properties":{}}"#.to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
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

/// Inserts one real, committed row directly - bypassing `EventCache`
/// entirely (no `append` call), the same "simulates a writer this
/// process's own cache never heard about" role
/// `submit_command.rs::insert_concurrent_event` already plays for DCB
/// conflict tests.
async fn insert_event_bypassing_cache(pool: &Pool, bc: &BoundedContext, et: &EventType) -> Event {
    insert_tagged_event_bypassing_cache(pool, bc, et, Vec::new()).await
}

async fn insert_tagged_event_bypassing_cache(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    tags: Vec<Tag>,
) -> Event {
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
            correlation_id: None,
            causation_id: None,
        },
        sequence: seq,
        tags,
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event(pool, &e, None).await.unwrap();
    e
}

fn account(id: &str) -> Tag {
    Tag {
        key: "account".to_string(),
        value: Some(id.to_string()),
    }
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

/// A zero-capacity cache - the natural way to turn it off - used to keep
/// no events yet report every bounded context as empty: reads served from
/// it (fetch, consume, queryEvents, catch-up) silently returned nothing,
/// after loading the whole history into the window and discarding it. A
/// zero-capacity cache is now a cache that's off: every lookup misses, so
/// callers read Postgres.
#[test]
fn a_zero_capacity_cache_misses_instead_of_claiming_nothing_exists() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        for _ in 0..3 {
            insert_event_bypassing_cache(&pool, &bc, &et).await;
        }
        let cache = EventCache::new(0);
        assert!(
            cache
                .try_events_after(&pool, &bc.name, -1)
                .await
                .unwrap()
                .is_none(),
            "a disabled cache must miss, not claim the context is empty"
        );
        let events = db::list_events_cached(&pool, &cache, &bc.name, &et.name, -1)
            .await
            .unwrap();
        assert_eq!(events.len(), 3);
    });
}

/// A cold window (a bounded context never warmed, e.g. added at runtime)
/// fills from the recent tail, like `warm`, instead of loading the whole
/// history to keep only `capacity` of it - and still answers a read
/// reaching further back as a miss.
#[test]
fn a_cold_window_fills_from_the_recent_tail() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let mut sequences = Vec::new();
        for _ in 0..5 {
            sequences.push(insert_event_bypassing_cache(&pool, &bc, &et).await.sequence);
        }
        let cache = EventCache::new(2);
        let tail = cache
            .try_events_after(&pool, &bc.name, sequences[2])
            .await
            .unwrap()
            .expect("the last two events are within the window");
        assert_eq!(
            tail.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            sequences[3..].to_vec()
        );
        assert!(cache
            .try_events_after(&pool, &bc.name, -1)
            .await
            .unwrap()
            .is_none());
    });
}

/// docs/architecture.md §89: instance A's window knows up to some event;
/// instance B commits the next one (A's cache never hears of it); A then
/// commits and appends its own. The window must not end up with a hole
/// that a read would serve as if complete, silently skipping B's event.
#[test]
fn appending_past_an_unseen_event_does_not_leave_a_hole() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let first = insert_event_bypassing_cache(&pool, &bc, &et).await;

        let instance_a = EventCache::new(1000);
        instance_a.warm(&pool, &bc.name).await.unwrap();

        let by_b = insert_event_bypassing_cache(&pool, &bc, &et).await;
        let by_a = insert_event_bypassing_cache(&pool, &bc, &et).await;
        instance_a.append(&by_a).await;

        let served: Vec<i64> = instance_a
            .try_events_after(&pool, &bc.name, first.sequence)
            .await
            .unwrap()
            .expect("covered")
            .iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(served, vec![by_b.sequence, by_a.sequence]);
    });
}

/// §89: a read's freshen can load an event from Postgres between its
/// commit and the committing instance's own `append`; the late append
/// must not add it a second time.
#[test]
fn a_late_append_of_an_already_freshened_event_is_not_a_duplicate() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;

        let cache = EventCache::new(1000);
        cache.warm(&pool, &bc.name).await.unwrap();
        let event = insert_event_bypassing_cache(&pool, &bc, &et).await;
        // A read freshens first...
        cache.try_events_after(&pool, &bc.name, -1).await.unwrap();
        // ...then the committing path's append arrives.
        cache.append(&event).await;

        let served: Vec<i64> = cache
            .try_events_after(&pool, &bc.name, -1)
            .await
            .unwrap()
            .expect("covered")
            .iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(served, vec![event.sequence]);
    });
}

/// docs/architecture.md §95: hard-deleting a bounded context frees its name
/// for reuse, and the cache keys windows by name. A window left from the
/// deleted context must never be served for its successor - neither while
/// the new one has fewer events than the old window, nor once it passes it.
#[test]
fn a_recreated_bounded_context_never_sees_its_predecessors_cached_events() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        for _ in 0..3 {
            insert_event_bypassing_cache(&pool, &bc, &et).await;
        }
        let cache = EventCache::new(1000);
        cache.warm(&pool, &bc.name).await.unwrap();
        assert_eq!(
            cache
                .try_events_after(&pool, &bc.name, -1)
                .await
                .unwrap()
                .unwrap()
                .len(),
            3
        );

        db::hard_delete_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        db::insert_bounded_context(&pool, &bc).await.unwrap();
        let et = seed_event_type(&pool, &bc).await;
        let first = insert_event_bypassing_cache(&pool, &bc, &et).await;

        let served = |cache: EventCache| {
            let pool = pool.clone();
            let name = bc.name.clone();
            async move {
                cache
                    .try_events_after(&pool, &name, -1)
                    .await
                    .unwrap()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|e| (e.sequence, e.metadata.created_at))
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(
            served(cache.clone()).await,
            vec![(first.sequence, first.metadata.created_at)],
            "fewer events than the old window"
        );

        let mut expected = vec![(first.sequence, first.metadata.created_at)];
        for _ in 0..4 {
            let e = insert_event_bypassing_cache(&pool, &bc, &et).await;
            expected.push((e.sequence, e.metadata.created_at));
        }
        assert_eq!(served(cache).await, expected, "past the old window");
    });
}

/// docs/architecture.md §180 (Codeberg #67): `private_fields` is a live
/// declaration - declaring a field private hides it across the type's
/// whole history at once. A cached event used to keep the copy of its
/// type it was cached with, so after a re-registration (here by another
/// instance, which this cache never hears of) REST still served the field
/// in the clear from the cache, while the same read from Postgres redacted
/// it.
#[test]
fn a_re_registration_applies_to_events_already_cached() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let earlier = insert_event_bypassing_cache(&pool, &bc, &et).await;
        let seq = db::next_sequence(&pool, &bc.name).await.unwrap();
        let event = Event {
            payload: r#"{"note":"for my eyes only"}"#.to_string(),
            sequence: seq,
            ..earlier
        };
        db::insert_event(&pool, &event, None).await.unwrap();

        let cache = EventCache::new(1000);
        cache.warm(&pool, &bc.name).await.unwrap();
        assert!(cache
            .try_events_after(&pool, &bc.name, -1)
            .await
            .unwrap()
            .unwrap()
            .iter()
            .all(|e| e.event_type.private_fields.is_empty()));

        let private = EventType {
            schema: r#"{"properties":{"note":{"type":"string"}}}"#.to_string(),
            private_fields: vec![PrivateField {
                field: "note".to_string(),
                kind: PrivateFieldKind::Own,
                team: None,
                addressee_field: None,
            }],
            ..et.clone()
        };
        db::upsert_event_type(&pool, &private).await.unwrap();

        let served = cache
            .try_events_after(&pool, &bc.name, -1)
            .await
            .unwrap()
            .expect("still covered");
        let served = served.iter().find(|e| e.sequence == seq).unwrap();
        assert_eq!(served.event_type.private_fields, private.private_fields);
        assert_eq!(
            skilj_core::event_store::redact_private_fields(served).payload,
            r#"{"note":null}"#
        );
        let inspected = cache
            .try_event_by_sequence(&pool, &bc.name, seq)
            .await
            .unwrap()
            .expect("cached");
        assert_eq!(inspected.event_type.private_fields, private.private_fields);
    });
}

/// The registrations stamp the cache compares on every read leaves out
/// what a scheduled fire moves, so a type firing every second doesn't
/// make the cache reload its registrations on every read (§180).
#[test]
fn a_scheduled_fire_does_not_change_the_registrations_stamp() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let scheduled = EventType {
            system_triggered_allowed: true,
            system_triggered_schedule: Some("0 * * * * *".to_string()),
            missed_occurrence_policy: Some(skilj_core::event_store::MissedOccurrencePolicy::Skip),
            schedule_position: Some(test_now() - chrono::Duration::hours(1)),
            ..et
        };
        db::upsert_event_type(&pool, &scheduled).await.unwrap();
        let before = db::events_table_identity(&pool, &bc.name).await.unwrap();

        let advanced = db::skip_missed_occurrences_for_event_type(
            &pool,
            &bc.name,
            &scheduled.name,
            test_now(),
        )
        .await
        .unwrap();
        assert!(advanced.is_some(), "the position must actually have moved");
        let after = db::events_table_identity(&pool, &bc.name).await.unwrap();
        assert_eq!(before.registrations, after.registrations);

        db::upsert_event_type(
            &pool,
            &EventType {
                event_read_allowed: false,
                ..scheduled
            },
        )
        .await
        .unwrap();
        let reregistered = db::events_table_identity(&pool, &bc.name).await.unwrap();
        assert_ne!(after.registrations, reregistered.registrations);
    });
}

/// docs/architecture.md §187 (Codeberg #50): a tag read returns only the
/// matching events, and says it is complete through the window's highest
/// sequence - matching or not - which is where a command's re-check under
/// the lock starts (§178).
#[test]
fn a_tag_read_serves_the_matching_events_complete_through_the_whole_window() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let a1 = insert_tagged_event_bypassing_cache(&pool, &bc, &et, vec![account("a")]).await;
        insert_tagged_event_bypassing_cache(&pool, &bc, &et, vec![account("b")]).await;
        let a2 =
            insert_tagged_event_bypassing_cache(&pool, &bc, &et, vec![account("b"), account("a")])
                .await;
        let last = insert_event_bypassing_cache(&pool, &bc, &et).await;

        let cache = EventCache::new(1000);
        cache.warm(&pool, &bc.name).await.unwrap();
        let (events, covered_through) = cache
            .try_events_matching_tags(&pool, &bc.name, &[account("a")], -1)
            .await
            .unwrap()
            .expect("the window holds the whole history");
        assert_eq!(
            events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![a1.sequence, a2.sequence]
        );
        assert_eq!(covered_through, last.sequence);

        let (events, covered_through) = cache
            .try_events_matching_tags(&pool, &bc.name, &[account("nobody")], -1)
            .await
            .unwrap()
            .unwrap();
        assert!(events.is_empty());
        assert_eq!(covered_through, last.sequence);
    });
}

/// docs/architecture.md §187 (Codeberg #50): once a window has evicted
/// the first event, a tag read - which needs the whole history - misses
/// without the freshen round trip it used to pay first. Shown with a pool
/// that is closed by then: the tag read still misses cleanly, while a
/// read the window could serve has to reach the database and fails.
#[test]
fn a_tag_read_past_the_windows_first_event_misses_without_a_round_trip() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        for _ in 0..5 {
            insert_tagged_event_bypassing_cache(&pool, &bc, &et, vec![account("a")]).await;
        }
        let own_pool = db::PgPoolOptions::new()
            .max_connections(1)
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap();
        let cache = EventCache::new(3);
        cache.warm(&own_pool, &bc.name).await.unwrap();
        own_pool.close().await;

        assert_eq!(
            cache
                .try_events_matching_tags(&own_pool, &bc.name, &[account("a")], -1)
                .await
                .unwrap(),
            None
        );
        assert!(cache
            .try_events_after(&own_pool, &bc.name, 3)
            .await
            .is_err());
    });
}

/// docs/architecture.md §188 (Codeberg #51): a command deciding from a
/// snapshot only needs the events after the snapshot's `as_of_sequence`,
/// which a window that no longer reaches the first event can still hold.
/// It used to miss whenever the window didn't reach the start, so a
/// long-lived bounded context's snapshot commands always read Postgres.
#[test]
fn a_tag_read_after_a_position_is_served_by_a_window_that_holds_it() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let mut events = Vec::new();
        for i in 0..6 {
            let id = if i % 2 == 0 { "a" } else { "b" };
            events.push(
                insert_tagged_event_bypassing_cache(&pool, &bc, &et, vec![account(id)]).await,
            );
        }
        // Holds sequences 2-5.
        let cache = EventCache::new(4);
        cache.warm(&pool, &bc.name).await.unwrap();
        let read = |after: i64| {
            let cache = cache.clone();
            let pool = pool.clone();
            let name = bc.name.clone();
            async move {
                cache
                    .try_events_matching_tags(&pool, &name, &[account("a")], after)
                    .await
                    .unwrap()
                    .map(|(events, covered_through)| {
                        (
                            events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
                            covered_through,
                        )
                    })
            }
        };

        assert_eq!(
            read(events[1].sequence).await,
            Some((
                vec![events[2].sequence, events[4].sequence],
                events[5].sequence
            )),
            "from the window's own first event"
        );
        assert_eq!(
            read(events[2].sequence).await,
            Some((vec![events[4].sequence], events[5].sequence))
        );
        assert_eq!(
            read(events[5].sequence).await,
            Some((Vec::new(), events[5].sequence)),
            "nothing since"
        );
        assert_eq!(
            read(events[0].sequence).await,
            None,
            "needs an evicted event"
        );
        assert_eq!(read(-1).await, None, "needs the whole history");
    });
}
