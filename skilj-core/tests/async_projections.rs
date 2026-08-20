//! Tests for `db::catch_up_bounded_context`/`db::promote_projection_rebuild`,
//! the async half of §8 item 6 (see the plan at
//! `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md` and
//! docs/architecture.md's own write-up). No `skilj-graphql`/`skilj-rest`/
//! `SkiljBuilder` involved - a hand-rolled `ProjectionDispatcher` test
//! double exercises the persistence mechanism directly, called by hand
//! instead of by the background task `SkiljBuilder::build()` spawns
//! (`skilj/tests/async_projections.rs` covers that half end-to-end). Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `sync_projections.rs` - see its own doc comment for the details, not
//! repeated a third time here.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::ProjectionDispatcher;
use skilj_core::projections::{Projection, ProjectionRebuild, ProjectionRebuildStatus};
use skilj_core::shared::{generate_token_id, Metadata};

/// Same registered projection `sync_projections.rs`'s own `TestDispatcher`
/// has (`"AccountBalance"`), single implicit key throughout - this file's
/// own coverage is the async/rebuild machinery, not keyed projections
/// (already covered directly by `sync_projections.rs`'s own
/// `TransferBalances`), so there's no need to duplicate that here too.
struct TestDispatcher;

impl ProjectionDispatcher for TestDispatcher {
    fn keys(
        &self,
        _bounded_context: &str,
        projection_name: &str,
        event: &Event,
    ) -> Option<Vec<String>> {
        match projection_name {
            "AccountBalance" if event.event_type.name == "MoneyDeposited" => {
                Some(vec![String::new()])
            }
            "AccountBalance" => Some(Vec::new()),
            _ => None,
        }
    }

    fn project(
        &self,
        _bounded_context: &str,
        projection_name: &str,
        state_json: &str,
        event: &Event,
        _key: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        match projection_name {
            "AccountBalance" => {
                if event.event_type.name != "MoneyDeposited" {
                    return Some(Ok(state_json.to_string()));
                }
                let current: i64 = state_json
                    .parse()
                    .expect("test state is always a plain i64");
                let payload: serde_json::Value =
                    serde_json::from_str(&event.payload).expect("test payload is always JSON");
                let amount = payload["amount"].as_i64().unwrap_or(0);
                Some(Ok((current + amount).to_string()))
            }
            _ => None,
        }
    }

    fn default_state(&self, _bounded_context: &str, projection_name: &str) -> Option<String> {
        match projection_name {
            "AccountBalance" => Some("0".to_string()),
            _ => None,
        }
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
            .expect("failed to build a tokio runtime for async_projections tests")
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
    let database_name = "skilj_async_projections_test";
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
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    bc
}

async fn seed_event_type(pool: &Pool, bc: &BoundedContext, name: &str) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: name.to_string(),
        schema: r#"{"properties":{"amount":{"type":"number"}}}"#.to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
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

/// Registers a `sync = false` `Projection` consuming `consumed` - no
/// `projection_state` seeding anymore (§9's "keyed / multi-row
/// Projections" pass): each instance's own row is created lazily, on
/// first touch, starting from `TestDispatcher::default_state`'s own
/// `"0"`, the same shape `SkiljBuilder::projection::<T>()`'s own
/// reconciliation gives a real `T::State::default()`.
async fn seed_async_projection(
    pool: &Pool,
    bc: &BoundedContext,
    name: &str,
    consumed: Vec<EventType>,
) -> Projection {
    let projection = Projection {
        bounded_context: bc.clone(),
        name: name.to_string(),
        schema: r#"{"properties":{}}"#.to_string(),
        schema_version: 1,
        consumed_event_types: consumed,
        sync: false,
        caught_up_to: None,
    };
    db::upsert_projection(pool, &projection).await.unwrap();
    projection
}

fn event(bc: &BoundedContext, et: &EventType, sequence: i64, amount: i64) -> Event {
    Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: format!(r#"{{"amount":{amount}}}"#),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: "someone".to_string(),
            created_at: test_now(),
        },
        sequence,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

async fn insert_plain_event(pool: &Pool, bc: &BoundedContext, et: &EventType, amount: i64) -> i64 {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let e = event(bc, et, seq, amount);
    db::insert_event(pool, &e, None).await.unwrap();
    seq
}

/// Unlike `insert_plain_event` above, this goes through the *locked*
/// production shape every real event-creation call site uses
/// (`create_and_insert_direct_event` etc.) - `next_sequence` and the
/// event's own insert share one transaction, so the bounded context's
/// `sequence` row stays locked for the whole call, not just the
/// increment. Needed by the concurrency test below, which specifically
/// needs that lock actually held for its own race against
/// `promote_projection_rebuild` to be real - drift audit finding #6's
/// fix depends entirely on both sides taking it.
async fn insert_event_via_the_locked_path(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    amount: i64,
) -> i64 {
    let mut tx = pool.begin().await.unwrap();
    let seq = db::next_sequence(&mut *tx, &bc.name).await.unwrap();
    let e = event(bc, et, seq, amount);
    db::insert_event_and_update_sync_projections_in_tx(
        pool,
        &mut tx,
        &e,
        None,
        &TestDispatcher,
        &[],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    seq
}

#[test]
fn a_cold_start_catch_up_folds_every_existing_event() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc, "MoneyDeposited").await;
        seed_async_projection(&pool, &bc, "AccountBalance", vec![et.clone()]).await;

        insert_plain_event(&pool, &bc, &et, 20).await;
        let last_seq = insert_plain_event(&pool, &bc, &et, 5).await;

        db::catch_up_bounded_context(&pool, &bc.name, &TestDispatcher)
            .await
            .unwrap();

        let state = db::get_projection_state(&pool, &bc.name, "AccountBalance", "")
            .await
            .unwrap();
        assert_eq!(state, Some("25".to_string()));

        let projection = db::get_projection(&pool, &bc.name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projection.caught_up_to, Some(last_seq));
    });
}

#[test]
fn a_second_tick_with_no_new_events_is_a_no_op() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc, "MoneyDeposited").await;
        seed_async_projection(&pool, &bc, "AccountBalance", vec![et.clone()]).await;
        let last_seq = insert_plain_event(&pool, &bc, &et, 20).await;

        db::catch_up_bounded_context(&pool, &bc.name, &TestDispatcher)
            .await
            .unwrap();
        db::catch_up_bounded_context(&pool, &bc.name, &TestDispatcher)
            .await
            .unwrap();

        let state = db::get_projection_state(&pool, &bc.name, "AccountBalance", "")
            .await
            .unwrap();
        assert_eq!(state, Some("20".to_string()));
        let projection = db::get_projection(&pool, &bc.name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projection.caught_up_to, Some(last_seq));
    });
}

#[test]
fn a_building_rebuild_replays_from_the_start_and_promotes_once_caught_up() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc, "MoneyDeposited").await;

        // The live projection starts out already partially caught up (it
        // was consuming events before the rebuild was staged) - the
        // rebuild's own replay starts fresh from the beginning regardless,
        // per `RegisterProjection`'s own "a rebuild starts from nothing"
        // rule; this is exactly what makes a rebuild's own
        // `projection_rebuild_state` row necessary in the first place
        // (it can't share the live projection's already-advanced one).
        let existing = seed_async_projection(&pool, &bc, "AccountBalance", vec![et.clone()]).await;
        let first_seq = insert_plain_event(&pool, &bc, &et, 20).await;
        db::catch_up_bounded_context(&pool, &bc.name, &TestDispatcher)
            .await
            .unwrap();
        let mid_projection = db::get_projection(&pool, &bc.name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mid_projection.caught_up_to, Some(first_seq));
        assert_eq!(
            db::get_projection_state(&pool, &bc.name, "AccountBalance", "")
                .await
                .unwrap(),
            Some("20".to_string())
        );

        let rebuild = ProjectionRebuild {
            projection: existing.clone(),
            schema: r#"{"properties":{"total":{"type":"integer"}}}"#.to_string(),
            schema_version: 2,
            consumed_event_types: vec![et.clone()],
            sync: false,
            caught_up_to: None,
            status: ProjectionRebuildStatus::Building,
        };
        db::upsert_projection_rebuild(&pool, &rebuild)
            .await
            .unwrap();

        // A coexisting pending rebuild, staged alongside the building one
        // rather than disturbing it (UniqueRebuildPerProjectionAndStatus's
        // own "deliberate case") - promotion below must leave this one
        // alone.
        let pending = ProjectionRebuild {
            schema_version: 3,
            status: ProjectionRebuildStatus::Pending,
            caught_up_to: None,
            ..rebuild.clone()
        };
        db::upsert_projection_rebuild(&pool, &pending)
            .await
            .unwrap();

        let last_seq = insert_plain_event(&pool, &bc, &et, 5).await;

        db::catch_up_bounded_context(&pool, &bc.name, &TestDispatcher)
            .await
            .unwrap();

        // Promoted: no *building* rebuild row left, and the live
        // projection now carries the rebuild's own schema/version and the
        // state folded from both events (20 + 5), not just the one folded
        // after staging - "starts from nothing" replaying the whole
        // history, not resuming the live projection's own prior progress.
        assert!(db::get_projection_rebuild(
            &pool,
            &bc.name,
            "AccountBalance",
            ProjectionRebuildStatus::Building
        )
        .await
        .unwrap()
        .is_none());
        // The coexisting pending rebuild survives the promotion untouched
        // - compared field-by-field, not via a whole-struct `assert_eq!`,
        // since `pending.projection` (embedded, captured before this
        // promotion ran) is now stale on its own `schema_version`/
        // `caught_up_to` - `get_projection_rebuild` always re-attaches the
        // *live*, current `Projection` row, which promotion just changed.
        let reloaded_pending = db::get_projection_rebuild(
            &pool,
            &bc.name,
            "AccountBalance",
            ProjectionRebuildStatus::Pending,
        )
        .await
        .unwrap()
        .expect("the coexisting pending rebuild must survive promotion");
        assert_eq!(reloaded_pending.schema, pending.schema);
        assert_eq!(reloaded_pending.schema_version, pending.schema_version);
        assert_eq!(
            reloaded_pending.consumed_event_types,
            pending.consumed_event_types
        );
        assert_eq!(reloaded_pending.sync, pending.sync);
        assert_eq!(reloaded_pending.caught_up_to, pending.caught_up_to);
        assert_eq!(reloaded_pending.status, pending.status);

        let promoted = db::get_projection(&pool, &bc.name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(promoted.schema, rebuild.schema);
        assert_eq!(promoted.schema_version, 2);
        assert_eq!(promoted.caught_up_to, Some(last_seq));

        assert_eq!(
            db::get_projection_state(&pool, &bc.name, "AccountBalance", "")
                .await
                .unwrap(),
            Some("25".to_string())
        );
    });
}

/// Drift audit finding #6 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): a rebuild whose own `caught_up_to`
/// no longer matches the bounded context's real latest sequence -
/// because a new event committed after the fold pass that produced it -
/// must defer promotion rather than strand that event. Constructs the
/// stale state directly (a real `catch_up_bounded_context` fold pass
/// would take an extra tick to reach the same point; asserting on
/// `promote_projection_rebuild`'s own return value directly is a more
/// exact proof of the deferral logic itself than routing through that
/// extra layer), then proves the deferred rebuild is not stuck forever -
/// the very next tick folds the new event in and promotes for real.
#[test]
fn promotion_defers_instead_of_stranding_an_event_committed_after_the_fold_pass() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc, "MoneyDeposited").await;
        let existing = seed_async_projection(&pool, &bc, "AccountBalance", vec![et.clone()]).await;

        let first_seq = insert_plain_event(&pool, &bc, &et, 20).await;

        // A building rebuild already fully caught up to first_seq -
        // exactly what catch_up_bounded_context's own fold loop would
        // have just produced, immediately eligible for promotion.
        // sync: true - this is specifically the async->sync promotion
        // finding #6 is about; ProjectionRebuild.sync is the target state
        // to promote *to*, not a description of the rebuild's own nature.
        let rebuild = ProjectionRebuild {
            projection: existing,
            schema: r#"{"properties":{"total":{"type":"integer"}}}"#.to_string(),
            schema_version: 2,
            consumed_event_types: vec![et.clone()],
            sync: true,
            caught_up_to: Some(first_seq),
            status: ProjectionRebuildStatus::Building,
        };
        db::upsert_projection_rebuild(&pool, &rebuild)
            .await
            .unwrap();

        // The race: a second event commits after that fold pass, before
        // promotion runs.
        let second_seq = insert_plain_event(&pool, &bc, &et, 5).await;

        let promoted = db::promote_projection_rebuild(&pool, &bc.name, "AccountBalance")
            .await
            .unwrap();
        assert!(
            !promoted,
            "promotion must defer, not strand second_seq's own event"
        );

        // Deferred, not lost: the building rebuild survives untouched,
        // and the live projection is still async - neither side silently
        // dropped the new event.
        let still_building = db::get_projection_rebuild(
            &pool,
            &bc.name,
            "AccountBalance",
            ProjectionRebuildStatus::Building,
        )
        .await
        .unwrap()
        .expect("a deferred promotion must leave the building rebuild in place");
        assert_eq!(still_building.caught_up_to, Some(first_seq));
        let still_async = db::get_projection(&pool, &bc.name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert!(!still_async.sync);

        // The next tick: folds second_seq into the still-building
        // rebuild, then promotes for real - self-correcting, not stuck.
        db::catch_up_bounded_context(&pool, &bc.name, &TestDispatcher)
            .await
            .unwrap();

        assert!(db::get_projection_rebuild(
            &pool,
            &bc.name,
            "AccountBalance",
            ProjectionRebuildStatus::Building
        )
        .await
        .unwrap()
        .is_none());
        let promoted_projection = db::get_projection(&pool, &bc.name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert!(promoted_projection.sync);
        assert_eq!(promoted_projection.caught_up_to, Some(second_seq));
        // Position only, deliberately - this test's own rebuild fixture
        // starts from a synthetic caught_up_to rather than a real fold
        // pass, so its own projection_rebuild_state was never seeded to
        // match; state-fold correctness after a real replay is already
        // covered by a_building_rebuild_replays_from_the_start_and_promotes_once_caught_up
        // above. What this test proves is that caught_up_to ends up
        // exactly at second_seq, not stranded at first_seq.
    });
}

/// The same finding, proven under a genuine concurrent race rather than
/// a hand-sequenced one - `tokio::join!`, the same real-concurrency
/// pattern `admin_context_bootstrap.rs`'s own
/// `two_concurrent_builds_against_a_fresh_database_both_succeed` uses.
/// Whichever side actually wins the bounded context's own `sequence` row
/// lock varies run to run - that's real, uncontrolled scheduling, not
/// something this test tries to pin - but the *final* state must be
/// identical either way: fully promoted, fully caught up, nothing
/// stranded. A follow-up `catch_up_bounded_context` call after the race
/// (standing in for "the next scheduled tick", exactly like the deferral
/// test above) is what makes that true regardless of which side won -
/// without the fix, this would only converge on one of the two possible
/// orderings, not both.
#[test]
fn a_real_concurrent_event_and_promotion_never_strand_the_event() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc, "MoneyDeposited").await;
        let existing = seed_async_projection(&pool, &bc, "AccountBalance", vec![et.clone()]).await;

        let first_seq = insert_plain_event(&pool, &bc, &et, 20).await;

        let rebuild = ProjectionRebuild {
            projection: existing,
            schema: r#"{"properties":{"total":{"type":"integer"}}}"#.to_string(),
            schema_version: 2,
            consumed_event_types: vec![et.clone()],
            sync: true,
            caught_up_to: Some(first_seq),
            status: ProjectionRebuildStatus::Building,
        };
        db::upsert_projection_rebuild(&pool, &rebuild)
            .await
            .unwrap();

        let (second_seq, promoted) = tokio::join!(
            insert_event_via_the_locked_path(&pool, &bc, &et, 5),
            db::promote_projection_rebuild(&pool, &bc.name, "AccountBalance"),
        );
        // Whichever side won, this must not have errored - a real `Err`
        // here (as opposed to `Ok(false)`, the legitimate "deferred"
        // outcome) would be a genuine failure worth surfacing, not
        // something to silently swallow.
        promoted.unwrap();

        db::catch_up_bounded_context(&pool, &bc.name, &TestDispatcher)
            .await
            .unwrap();

        assert!(db::get_projection_rebuild(
            &pool,
            &bc.name,
            "AccountBalance",
            ProjectionRebuildStatus::Building
        )
        .await
        .unwrap()
        .is_none());
        let final_projection = db::get_projection(&pool, &bc.name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert!(final_projection.sync);
        // Position only, deliberately - see the deferral test above's own
        // identical comment for why (a synthetic caught_up_to, not a real
        // fold pass, backs this fixture's rebuild).
        assert_eq!(final_projection.caught_up_to, Some(second_seq));
    });
}
