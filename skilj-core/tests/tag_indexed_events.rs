//! Tests for `db::list_events_for_bounded_context_matching_tags` -
//! [docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 1" fix: a GIN-indexed,
//! `tags @> ...`-based query replacing the "fetch the whole bounded
//! context, filter in memory" shape `list_events_for_bounded_context`/
//! `consistency_boundary_and_matching_events` used to be the only way to
//! get a tag-scoped `matching_events` set. Same "test the layer in
//! isolation" shape `skilj-core/tests/submit_command.rs` already uses
//! for `db::submit_command` - see that file's own doc comment for the
//! harness details, not repeated a third time here.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::shared::{generate_token_id, Metadata, Tag, TagMapping};

struct TestDb {
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for tag_indexed_events tests")
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

    let url = skilj_test_support::database_url("skilj_tag_indexed_events_test").await?;
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

/// Two independent tag keys - `order`/`customer` - deliberately, the
/// same shape `courses.rs`'s real `EnrollStudentInCourse` uses
/// (`student`/`course`) to prove union-of-tags matching, not just a
/// single tag key.
async fn seed_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "SomethingHappened".to_string(),
        schema: r#"{"properties":{"order_id":{"type":"string"},"customer_id":{"type":"string"}}}"#
            .to_string(),
        schema_version: 1,
        tag_mappings: vec![
            TagMapping {
                key: "order".to_string(),
                field: "order_id".to_string(),
            },
            TagMapping {
                key: "customer".to_string(),
                field: "customer_id".to_string(),
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

/// Inserted directly (bypassing `submit_command`), tagged with whichever
/// `tags` the caller supplies - lets each test build exactly the tag
/// combination it needs without a payload/tag_mappings round trip.
async fn insert_tagged_event(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    tags: Vec<Tag>,
) -> i64 {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let e = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: "{}".to_string(),
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

fn tag(key: &str, value: &str) -> Tag {
    Tag {
        key: key.to_string(),
        value: Some(value.to_string()),
    }
}

#[test]
fn returns_only_events_sharing_a_wanted_tag() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;

        let wanted_seq = insert_tagged_event(&pool, &bc, &et, vec![tag("order", "A")]).await;
        insert_tagged_event(&pool, &bc, &et, vec![tag("order", "B")]).await;
        insert_tagged_event(&pool, &bc, &et, vec![tag("customer", "X")]).await;

        let events = db::list_events_for_bounded_context_matching_tags(
            &pool,
            &bc.name,
            &[tag("order", "A")],
            None,
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, wanted_seq);
    });
}

/// The DCB-defining case: a command tagging on *two* keys (`courses.rs`'s
/// real `EnrollStudentInCourse` unions `student` and `course`) must get
/// back the union of both, not their intersection.
#[test]
fn unions_multiple_wanted_tags_rather_than_intersecting_them() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;

        let order_only = insert_tagged_event(&pool, &bc, &et, vec![tag("order", "A")]).await;
        let customer_only = insert_tagged_event(&pool, &bc, &et, vec![tag("customer", "X")]).await;
        insert_tagged_event(&pool, &bc, &et, vec![tag("order", "B")]).await;

        let mut events = db::list_events_for_bounded_context_matching_tags(
            &pool,
            &bc.name,
            &[tag("order", "A"), tag("customer", "X")],
            None,
        )
        .await
        .unwrap();
        events.sort_by_key(|e| e.sequence);

        assert_eq!(
            events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![order_only, customer_only]
        );
    });
}

/// An event tagged with *both* wanted keys (a real event genuinely
/// belonging to both entities) must appear exactly once, not twice -
/// each `tags @> $N::jsonb` clause is `OR`'d, and Postgres itself
/// dedups matching rows for a single `WHERE`, but worth proving
/// directly since the whole point of a union is "no duplicates."
#[test]
fn an_event_matching_both_wanted_tags_is_returned_exactly_once() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;

        let both = insert_tagged_event(
            &pool,
            &bc,
            &et,
            vec![tag("order", "A"), tag("customer", "X")],
        )
        .await;

        let events = db::list_events_for_bounded_context_matching_tags(
            &pool,
            &bc.name,
            &[tag("order", "A"), tag("customer", "X")],
            None,
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, both);
    });
}

#[test]
fn respects_after_sequence() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;

        let first = insert_tagged_event(&pool, &bc, &et, vec![tag("order", "A")]).await;
        let second = insert_tagged_event(&pool, &bc, &et, vec![tag("order", "A")]).await;

        let events = db::list_events_for_bounded_context_matching_tags(
            &pool,
            &bc.name,
            &[tag("order", "A")],
            Some(first),
        )
        .await
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, second);
    });
}

#[test]
fn results_are_ordered_by_sequence() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;

        // Inserted in an order that would come back wrong if the query
        // relied on insertion order instead of an explicit ORDER BY.
        let third = insert_tagged_event(&pool, &bc, &et, vec![tag("order", "A")]).await;
        insert_tagged_event(&pool, &bc, &et, vec![tag("customer", "irrelevant")]).await;
        let second = insert_tagged_event(&pool, &bc, &et, vec![tag("order", "A")]).await;

        let events = db::list_events_for_bounded_context_matching_tags(
            &pool,
            &bc.name,
            &[tag("order", "A")],
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![third, second]
        );
    });
}

/// Mirrors `consistency_boundary_and_matching_events`'s own "no
/// `tag_mappings` declared" case exactly - see this function's own doc
/// comment for why this must never touch Postgres at all, not just
/// return an empty result.
#[test]
fn an_empty_tags_slice_returns_nothing_without_a_bounded_context_row() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        // Deliberately not seeded - `get_bounded_context` would panic
        // (its own `.expect`) if this function tried to look it up, so
        // reaching `Ok(vec![])` here is itself proof the empty-tags case
        // short-circuits before any query.
        let events = db::list_events_for_bounded_context_matching_tags(
            &pool,
            "no-such-bounded-context",
            &[],
            None,
        )
        .await
        .unwrap();

        assert_eq!(events, Vec::<Event>::new());
    });
}
