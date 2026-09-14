//! Tests for `db::insert_event_and_update_sync_projections` and
//! `db::get_projection_state` - the real, transactional half of [§8](../../docs/architecture.md#open-for-a-future-pass) item 6
//! (`project()`, sync case only), now also covering [§9](../../docs/architecture.md#next-steps)'s "keyed /
//! multi-row Projections" pass - see `docs/architecture.md`'s own
//! write-up and the plan at
//! `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`. No
//! `skilj-graphql`/`skilj-rest` involved - a hand-rolled
//! `ProjectionDispatcher` test double exercises the persistence
//! mechanism directly, the same "test the layer in isolation" shape
//! `skilj-core/tests/persistence.rs`'s other round-trip tests already
//! use. Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! that file - see its own doc comment for the details, not repeated a
//! third time here.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::ProjectionDispatcher;
use skilj_core::projections::Projection;
use skilj_core::shared::{generate_token_id, Metadata};

/// A minimal `ProjectionDispatcher` test double - three registered
/// projections: `"AccountBalance"` (sums `amount` from `"MoneyDeposited"`
/// events only, single implicit key) and `"EventCount"` (increments on
/// every event, regardless of type, single implicit key) - enough to
/// exercise both "state actually changes" and "state passes through
/// unchanged, `caught_up_to` still advances" in the same harness; and
/// `"TransferBalances"` (keyed by account id - a `"Transferred"` event's
/// own `from`/`to` fields, crediting one and debiting the other from a
/// single fold) exercising [§9](../../docs/architecture.md#next-steps)'s "keyed / multi-row Projections" pass -
/// one event updating two independent rows.
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
            "EventCount" => Some(vec![String::new()]),
            "TransferBalances" if event.event_type.name == "Transferred" => {
                let payload: serde_json::Value =
                    serde_json::from_str(&event.payload).expect("test payload is always JSON");
                Some(vec![
                    payload["from"].as_str().unwrap_or_default().to_string(),
                    payload["to"].as_str().unwrap_or_default().to_string(),
                ])
            }
            "TransferBalances" => Some(Vec::new()),
            _ => None,
        }
    }

    fn project(
        &self,
        _bounded_context: &str,
        projection_name: &str,
        state_json: &str,
        event: &Event,
        key: &str,
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
            "EventCount" => {
                let current: i64 = state_json
                    .parse()
                    .expect("test state is always a plain i64");
                Some(Ok((current + 1).to_string()))
            }
            "TransferBalances" => {
                if event.event_type.name != "Transferred" {
                    return Some(Ok(state_json.to_string()));
                }
                let current: i64 = state_json
                    .parse()
                    .expect("test state is always a plain i64");
                let payload: serde_json::Value =
                    serde_json::from_str(&event.payload).expect("test payload is always JSON");
                let amount = payload["amount"].as_i64().unwrap_or(0);
                let from = payload["from"].as_str().unwrap_or_default();
                let to = payload["to"].as_str().unwrap_or_default();
                if key == from {
                    Some(Ok((current - amount).to_string()))
                } else if key == to {
                    Some(Ok((current + amount).to_string()))
                } else {
                    Some(Ok(state_json.to_string()))
                }
            }
            _ => None,
        }
    }

    fn default_state(&self, _bounded_context: &str, projection_name: &str) -> Option<String> {
        match projection_name {
            "AccountBalance" | "EventCount" | "TransferBalances" => Some("0".to_string()),
            _ => None,
        }
    }

    fn owner_tag_key(
        &self,
        _bounded_context: &str,
        projection_name: &str,
    ) -> Option<Option<&'static str>> {
        match projection_name {
            "AccountBalance" | "EventCount" | "TransferBalances" => Some(None),
            _ => None,
        }
    }

    fn team_only(
        &self,
        _bounded_context: &str,
        projection_name: &str,
    ) -> Option<Option<&'static str>> {
        match projection_name {
            "AccountBalance" | "EventCount" | "TransferBalances" => Some(None),
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
            .expect("failed to build a tokio runtime for sync_projections tests")
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
    let database_name = "skilj_sync_projections_test";
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

async fn seed_event_type(pool: &Pool, bc: &BoundedContext, name: &str) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: name.to_string(),
        schema: r#"{"properties":{"amount":{"type":"number"}}}"#.to_string(),
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

/// Registers a `sync = true` `Projection` consuming `consumed` - no
/// `projection_state` seeding anymore ([§9](../../docs/architecture.md#next-steps)'s "keyed / multi-row
/// Projections" pass): each instance's own row is created lazily, on
/// first touch, starting from `TestDispatcher::default_state`'s own
/// `"0"`, the same shape `SkiljBuilder::projection::<T>()`'s own
/// reconciliation gives a real `T::State::default()`.
async fn seed_sync_projection(
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
        sync: true,
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
            correlation_id: None,
            causation_id: None,
        },
        sequence,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

#[test]
fn a_consumed_event_updates_both_state_and_caught_up_to() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc, "MoneyDeposited").await;
        seed_sync_projection(&pool, &bc, "AccountBalance", vec![et.clone()]).await;

        let seq = db::next_sequence(&pool, &bc.name).await.unwrap();
        let e = event(&bc, &et, seq, 20);
        db::insert_event_and_update_sync_projections(
            &pool,
            &e,
            None,
            &TestDispatcher,
            &[],
            &skilj_core::event_store::EventBroadcaster::new(16),
            &skilj_core::event_cache::EventCache::new(1000),
        )
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
        assert_eq!(projection.caught_up_to, Some(seq));
    });
}

/// A non-consumed event still advances `caught_up_to` - "advances that
/// projection's own caught_up_to to the event's sequence either way"
/// (the note above the rules) - but touches no instance at all: `keys()`
/// itself returns none for an event type it doesn't consume, so no row
/// is even lazily created ([§9](../../docs/architecture.md#next-steps)'s "keyed / multi-row Projections" pass -
/// unlike the old always-pre-seeded schema, `get_projection_state` for a
/// key nothing has ever touched is genuinely `None`, not a default-valued
/// row; `resolvers::projection_query`'s own `default_state` fallback is
/// what a real caller sees instead of this raw `None`).
#[test]
fn an_unconsumed_event_advances_caught_up_to_but_touches_no_instance() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let money_deposited = seed_event_type(&pool, &bc, "MoneyDeposited").await;
        let other = seed_event_type(&pool, &bc, "SomethingElseHappened").await;
        seed_sync_projection(&pool, &bc, "AccountBalance", vec![money_deposited]).await;

        let seq = db::next_sequence(&pool, &bc.name).await.unwrap();
        let e = event(&bc, &other, seq, 999);
        db::insert_event_and_update_sync_projections(
            &pool,
            &e,
            None,
            &TestDispatcher,
            &[],
            &skilj_core::event_store::EventBroadcaster::new(16),
            &skilj_core::event_cache::EventCache::new(1000),
        )
        .await
        .unwrap();

        let state = db::get_projection_state(&pool, &bc.name, "AccountBalance", "")
            .await
            .unwrap();
        assert_eq!(state, None);

        let projection = db::get_projection(&pool, &bc.name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projection.caught_up_to, Some(seq));
    });
}

/// One event, two rows: a transfer event credits the receiver's own row
/// and debits the giver's, each independently, from a single fold -
/// [§9](../../docs/architecture.md#next-steps)'s own worked example ("keyed / multi-row Projections").
#[test]
fn a_transfer_event_updates_both_accounts_own_row_from_one_fold() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = EventType {
            bounded_context: bc.clone(),
            name: "Transferred".to_string(),
            schema: r#"{"properties":{"from":{"type":"string"},"to":{"type":"string"},"amount":{"type":"number"}}}"#.to_string(),
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
        db::upsert_event_type(&pool, &et).await.unwrap();
        seed_sync_projection(&pool, &bc, "TransferBalances", vec![et.clone()]).await;

        let seq = db::next_sequence(&pool, &bc.name).await.unwrap();
        let e = Event {
            bounded_context: bc.clone(),
            event_type: et.clone(),
            payload: r#"{"from":"alice","to":"bob","amount":15}"#.to_string(),
            metadata: Metadata {
                r#type: et.name.clone(),
                version: et.schema_version,
                client_id: "someone".to_string(),
                created_at: test_now(),
                correlation_id: None,
                causation_id: None,
            },
            sequence: seq,
            tags: Vec::new(),
            encryption_keys: Vec::new(),
            origin: EventOrigin::DirectlyCreated,
        };
        db::insert_event_and_update_sync_projections(
            &pool,
            &e,
            None,
            &TestDispatcher,
            &[],
            &skilj_core::event_store::EventBroadcaster::new(16),
            &skilj_core::event_cache::EventCache::new(1000),
        )
        .await
        .unwrap();

        assert_eq!(
            db::get_projection_state(&pool, &bc.name, "TransferBalances", "alice")
                .await
                .unwrap(),
            Some("-15".to_string())
        );
        assert_eq!(
            db::get_projection_state(&pool, &bc.name, "TransferBalances", "bob")
                .await
                .unwrap(),
            Some("15".to_string())
        );
        // A key nothing named - not touched, not even lazily created.
        assert_eq!(
            db::get_projection_state(&pool, &bc.name, "TransferBalances", "carol")
                .await
                .unwrap(),
            None
        );
    });
}

/// Two sync projections in the same bounded context both update off one
/// event write, in the same transaction.
#[test]
fn two_sync_projections_both_update_from_one_event() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc, "MoneyDeposited").await;
        seed_sync_projection(&pool, &bc, "AccountBalance", vec![et.clone()]).await;
        seed_sync_projection(&pool, &bc, "EventCount", vec![et.clone()]).await;

        let seq = db::next_sequence(&pool, &bc.name).await.unwrap();
        let e = event(&bc, &et, seq, 30);
        db::insert_event_and_update_sync_projections(
            &pool,
            &e,
            None,
            &TestDispatcher,
            &[],
            &skilj_core::event_store::EventBroadcaster::new(16),
            &skilj_core::event_cache::EventCache::new(1000),
        )
        .await
        .unwrap();

        assert_eq!(
            db::get_projection_state(&pool, &bc.name, "AccountBalance", "")
                .await
                .unwrap(),
            Some("30".to_string())
        );
        assert_eq!(
            db::get_projection_state(&pool, &bc.name, "EventCount", "")
                .await
                .unwrap(),
            Some("1".to_string())
        );
    });
}
