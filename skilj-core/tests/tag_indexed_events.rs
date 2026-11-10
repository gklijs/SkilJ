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
    let url = skilj_test_support::database_url("skilj_tag_indexed_events_test").await?;
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
                blind: false,
            },
            TagMapping {
                key: "customer".to_string(),
                field: "customer_id".to_string(),
                blind: false,
            },
        ],
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        blob_fields: Vec::new(),
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

/// docs/architecture.md §108: a tag-filtered read walks the tag index a
/// chunk at a time - never more than `chunk_size` events in hand, other
/// tags' events never included, every match visited exactly once in
/// sequence order, and an empty result still calls back once.
#[test]
fn a_tagged_read_walks_matches_in_bounded_chunks() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let mut expected = Vec::new();
        for i in 0..7 {
            expected.push(insert_tagged_event(&pool, &bc, &et, vec![tag("customer", "c1")]).await);
            if i % 2 == 0 {
                insert_tagged_event(&pool, &bc, &et, vec![tag("customer", "c2")]).await;
            }
        }

        let mut chunk_sizes = Vec::new();
        let mut seen = Vec::new();
        db::for_each_tagged_event_chunk(
            &pool,
            &bc.name,
            &[tag("customer", "c1")],
            -1,
            3,
            |chunk| {
                chunk_sizes.push(chunk.len());
                seen.extend(chunk.iter().map(|e| e.sequence));
                Ok(true)
            },
        )
        .await
        .unwrap();
        assert_eq!(chunk_sizes, vec![3, 3, 1]);
        assert_eq!(seen, expected);

        let mut calls = 0;
        db::for_each_tagged_event_chunk(
            &pool,
            &bc.name,
            &[tag("customer", "nobody")],
            -1,
            3,
            |chunk| {
                calls += 1;
                assert!(chunk.is_empty());
                Ok(true)
            },
        )
        .await
        .unwrap();
        assert_eq!(calls, 1);

        // `collect_tagged_event_page` stops as soon as the page is full.
        let page = db::collect_tagged_event_page(
            &pool,
            &bc.name,
            &[tag("customer", "c1")],
            expected[1],
            None,
            3,
            |chunk, room| Ok(chunk.iter().take(room).cloned().collect()),
        )
        .await
        .unwrap();
        assert_eq!(
            page.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            expected[2..5].to_vec()
        );

        // An as-of read (docs/architecture.md §237) serves nothing past its
        // bound, and doesn't ask for another chunk once it's passed it.
        let mut selects = 0;
        let page = db::collect_tagged_event_page(
            &pool,
            &bc.name,
            &[tag("customer", "c1")],
            -1,
            Some(expected[1]),
            2,
            |chunk, room| {
                selects += 1;
                Ok(chunk.iter().take(room).cloned().collect())
            },
        )
        .await
        .unwrap();
        assert_eq!(
            page.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            expected[..2].to_vec()
        );
        assert_eq!(selects, 1);
    });
}

/// One event of `et` on `bc`, not yet inserted.
fn event(bc: &BoundedContext, et: &EventType, sequence: i64, tags: Vec<Tag>) -> Event {
    Event {
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
        sequence,
        tags,
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

/// How often `event_tag_values` of `schema` has been read front to back, as
/// the statistics of `conn`'s own session have it once they are flushed.
async fn dictionary_scans(conn: &mut sqlx::PgConnection, schema_name: &str) -> i64 {
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query_scalar(
        "SELECT seq_scan FROM pg_stat_user_tables \
         WHERE schemaname = $1 AND relname = 'event_tag_values'",
    )
    .bind(schema_name)
    .fetch_one(&mut *conn)
    .await
    .unwrap()
}

/// The `(tag_key, tag_value, sequence)` rows `event_tags` has for `sequences`.
async fn indexed_tags(
    pool: &Pool,
    schema: &str,
    sequences: &[i64],
) -> Vec<(String, Option<String>, i64)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT tag_key, tag_value, sequence FROM {schema}.event_tags \
         WHERE sequence = ANY($1) ORDER BY sequence, tag_key"
    )))
    .bind(sequences)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Appending an event looks its tags up in the dictionary through the
/// dictionary's index, however many tags the bounded context has used
/// (docs/architecture.md §245). The trigger before this one read the whole
/// dictionary for every event; with 20,000 tags in it, PostgreSQL's
/// statistics count that as a sequential scan per insert.
#[test]
fn appending_an_event_does_not_scan_the_tag_dictionary() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let schema_name = format!("bc_{}", bc.name);
        let schema = format!("\"{schema_name}\"");
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.event_tag_values (tag_key, tag_value) \
             SELECT 'order', 'filler-' || g FROM generate_series(1, 20000) g; \
             ANALYZE {schema}.event_tag_values"
        )))
        .execute(&pool)
        .await
        .unwrap();

        // One session throughout, so that the counts read are its own.
        let mut conn = pool.acquire().await.unwrap();
        let before = dictionary_scans(&mut conn, &schema_name).await;
        let mut sequences = Vec::new();
        for n in 0..20 {
            let sequence = db::next_sequence(&pool, &bc.name).await.unwrap();
            let tags = vec![
                // In the dictionary already, new to it, and an explicit null.
                tag("order", &format!("filler-{}", n + 1)),
                tag("customer", &format!("c{n}")),
                Tag {
                    key: "region".to_string(),
                    value: None,
                },
            ];
            db::insert_event(&mut *conn, &event(&bc, &et, sequence, tags), None)
                .await
                .unwrap();
            sequences.push(sequence);
        }
        let after = dictionary_scans(&mut conn, &schema_name).await;
        assert_eq!(
            after, before,
            "appending 20 events read the whole tag dictionary"
        );

        let indexed = indexed_tags(&pool, &schema, &sequences).await;
        let expected: Vec<(String, Option<String>, i64)> = sequences
            .iter()
            .enumerate()
            .flat_map(|(n, &sequence)| {
                [
                    ("customer".to_string(), Some(format!("c{n}")), sequence),
                    (
                        "order".to_string(),
                        Some(format!("filler-{}", n + 1)),
                        sequence,
                    ),
                    ("region".to_string(), None, sequence),
                ]
            })
            .collect();
        assert_eq!(indexed, expected);
        let (tags, distinct): (i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT count(*), count(DISTINCT (tag_key, tag_value)) FROM {schema}.event_tag_values"
        )))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(tags, 20_000 + 20 + 1, "each new tag is added once");
        assert_eq!(distinct, tags);
    });
}

/// A bounded context whose tag index was built by skilj 0.3.0 has the
/// trigger function that scans the dictionary. Bringing it up to date
/// replaces that function, once, and leaves the index as it was
/// (docs/architecture.md §245).
#[test]
fn a_bounded_context_with_the_scanning_tag_trigger_gets_the_probing_one() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let schema = format!("\"bc_{}\"", bc.name);
        let first = insert_tagged_event(&pool, &bc, &et, vec![tag("order", "o1")]).await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            include_str!("fixtures/index_event_tags_0_3_0.sql").replace("@SCHEMA@", &schema),
        ))
        .execute(&pool)
        .await
        .unwrap();
        // The function's source, and the transaction that last wrote it.
        let function = || async {
            sqlx::query_as::<_, (String, String)>(sqlx::AssertSqlSafe(format!(
                "SELECT prosrc, xmin::text FROM pg_proc \
                 WHERE oid = to_regprocedure('{schema}.index_event_tags()')"
            )))
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        assert!(function().await.0.contains("NOT EXISTS"));

        db::ensure_event_tags_index(&pool, &bc.name).await.unwrap();
        let (source, written_by) = function().await;
        assert!(
            !source.contains("NOT EXISTS"),
            "still the scanning function"
        );

        // Nothing is rewritten for a bounded context that is up to date.
        db::ensure_event_tags_index(&pool, &bc.name).await.unwrap();
        assert_eq!(function().await.1, written_by);

        let second = insert_tagged_event(
            &pool,
            &bc,
            &et,
            vec![tag("order", "o1"), tag("customer", "c1")],
        )
        .await;
        assert_eq!(
            indexed_tags(&pool, &schema, &[first, second]).await,
            vec![
                ("order".to_string(), Some("o1".to_string()), first),
                ("customer".to_string(), Some("c1".to_string()), second),
                ("order".to_string(), Some("o1".to_string()), second),
            ]
        );
    });
}
