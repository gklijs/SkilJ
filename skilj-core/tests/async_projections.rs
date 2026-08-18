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

        let last_seq = insert_plain_event(&pool, &bc, &et, 5).await;

        db::catch_up_bounded_context(&pool, &bc.name, &TestDispatcher)
            .await
            .unwrap();

        // Promoted: no rebuild row left, and the live projection now
        // carries the rebuild's own schema/version and the state folded
        // from both events (20 + 5), not just the one folded after
        // staging - "starts from nothing" replaying the whole history,
        // not resuming the live projection's own prior progress.
        assert!(
            db::get_projection_rebuild(&pool, &bc.name, "AccountBalance")
                .await
                .unwrap()
                .is_none()
        );

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
