//! Tests for `db::submit_command` - the shared "locked half" of
//! `ProcessCommand` added closing the drift audit's #2/#3 findings (see
//! project memory `skilj-drift-audit-2026-08-18`): `next_sequence`'s row
//! lock, the DCB conflict re-check/redispatch, and the command+events
//! atomic insert, all in one transaction. `skilj-rest`/`skilj-graphql`
//! own end-to-end tests (`skilj/tests/`) prove the REST/GraphQL wiring;
//! these exercise `db::submit_command` and `db::create_and_insert_*_event`
//! directly, the same "test the layer in isolation" shape
//! `skilj-core/tests/sync_projections.rs` already uses for
//! `db::insert_event_and_update_sync_projections`. Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness as that file -
//! see its own doc comment for the details, not repeated a third time
//! here.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, CommandType, Event, EventBroadcaster, EventOrigin,
    EventType,
};
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher};
use skilj_core::projections::Projection;
use skilj_core::shared::{
    generate_token_id, CommandDecision, EventSpec, Metadata, Tag, TagMapping,
};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Keyed by `CommandType.name`, the same "one shared fixture, behaviour
/// selected by name" shape `sync_projections.rs`'s own `TestDispatcher`
/// uses for `projection_name`. `call_count` is what proves a DCB conflict
/// actually triggered a second `dispatch()` call, not just that the
/// final outcome happens to look right.
struct TestCommandDispatcher {
    calls: AtomicUsize,
}

impl TestCommandDispatcher {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl CommandDispatcher for TestCommandDispatcher {
    fn dispatch(
        &self,
        _bounded_context: &str,
        command_type: &str,
        payload: &str,
        matching_events: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match command_type {
            // Rejects once anything already matches its own consistency
            // tag - the "can't ship the same order twice" shape a real
            // DCB-guarded decider would have.
            "ShipOrder" => {
                if matching_events.is_empty() {
                    Some(Ok(CommandDecision::Accepted {
                        events: vec![EventSpec {
                            event_type: "OrderShipped".to_string(),
                            payload: serde_json::from_str(payload)
                                .expect("test payload is always JSON"),
                        }],
                    }))
                } else {
                    Some(Ok(CommandDecision::Rejected {
                        reason: "already shipped".to_string(),
                        kind: "already_shipped".to_string(),
                    }))
                }
            }
            // Always accepts, targeting an EventType this bounded
            // context never registers - `process_command`'s own
            // `UnregisteredEventType` failure mode.
            "TriggerUnregisteredEvent" => Some(Ok(CommandDecision::Accepted {
                events: vec![EventSpec {
                    event_type: "NeverRegistered".to_string(),
                    payload: serde_json::json!({}),
                }],
            })),
            // Always accepts two events - the second one is the
            // `TestProjectionDispatcher`'s own poison pill below.
            "TriggerTwoEvents" => Some(Ok(CommandDecision::Accepted {
                events: vec![
                    EventSpec {
                        event_type: "OrderShipped".to_string(),
                        payload: serde_json::json!({"order_id": "first"}),
                    },
                    EventSpec {
                        event_type: "PoisonEvent".to_string(),
                        payload: serde_json::json!({}),
                    },
                ],
            })),
            _ => None,
        }
    }

    fn required_role(
        &self,
        _bounded_context: &str,
        _command_type: &str,
    ) -> Option<Option<&'static str>> {
        Some(None)
    }

    // None of this file's fixtures opt into snapshotting - both new
    // methods just report "registered, no snapshot" the same way
    // `required_role` reports "registered, no extra gate" above.
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
        unreachable!("no fixture in this file opts into snapshot()")
    }
}

/// Registered as `sync = true`, consuming both `OrderShipped` and
/// `PoisonEvent` - fails for real on the second, the mechanism
/// `submit_command_rolls_back_the_command_and_every_event_together_when_a_later_event_fails_to_insert`
/// below needs to prove the whole transaction rolls back, not just the
/// one event that failed.
struct TestProjectionDispatcher;

impl ProjectionDispatcher for TestProjectionDispatcher {
    fn keys(
        &self,
        _bounded_context: &str,
        projection_name: &str,
        _event: &Event,
    ) -> Option<Vec<String>> {
        match projection_name {
            "Poisonable" => Some(vec![String::new()]),
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
            "Poisonable" if event.event_type.name == "PoisonEvent" => {
                Some(Err(skilj_core::event_store::Error::UnregisteredEventType(
                    "deliberate test poison pill".to_string(),
                )
                .into()))
            }
            "Poisonable" => Some(Ok(state_json.to_string())),
            _ => None,
        }
    }

    fn default_state(&self, _bounded_context: &str, projection_name: &str) -> Option<String> {
        match projection_name {
            "Poisonable" => Some("0".to_string()),
            _ => None,
        }
    }

    fn owner_tag_key(
        &self,
        _bounded_context: &str,
        projection_name: &str,
    ) -> Option<Option<&'static str>> {
        match projection_name {
            "Poisonable" => Some(None),
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
            .expect("failed to build a tokio runtime for submit_command tests")
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
    let database_name = "skilj_submit_command_test";
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

async fn seed_poison_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "PoisonEvent".to_string(),
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

/// Simulates a concurrent writer landing between the caller's own
/// optimistic read and `submit_command`'s own lock - a real committed
/// row, inserted directly (bypassing `submit_command` entirely), tagged
/// `order = tag_value`.
async fn insert_concurrent_event(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    tag_value: &str,
) {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let e = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: format!(r#"{{"order_id":"{tag_value}"}}"#),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: "concurrent-writer".to_string(),
            created_at: test_now(),
        },
        sequence: seq,
        tags: vec![Tag {
            key: "order".to_string(),
            value: Some(tag_value.to_string()),
        }],
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event(pool, &e, None).await.unwrap();
}

#[test]
fn submit_command_redispatches_and_rejects_on_a_genuine_dcb_conflict() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let payload = r#"{"order_id":"A"}"#;
        let consistency_tags = skilj_core::event_store::derive_tags(&ct.tag_mappings, payload);

        // The caller's own optimistic, unlocked read/dispatch - genuinely
        // stale: nothing has shipped order "A" yet as far as this read
        // is concerned.
        let bounded_context_events: Vec<Event> = Vec::new();
        let initial_decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();
        assert!(matches!(initial_decision, CommandDecision::Accepted { .. }));
        assert_eq!(dispatcher.call_count(), 1);

        // The "concurrent writer": order "A" ships for real, committed
        // before submit_command's own lock is acquired.
        insert_concurrent_event(&pool, &bc, &et, "A").await;

        let outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            payload,
            "client-1",
            &bounded_context_events,
            &consistency_tags,
            // The caller's own pre-lock matching_events - empty, same as
            // the `&[]` the initial dispatch() call above already used.
            &[],
            initial_decision,
            None,
            test_now(),
            None,
            None,
        )
        .await
        .unwrap();

        // Redispatched - the second call sees the concurrent event and
        // rejects, exactly the outcome a real DCB-guarded decider must
        // produce; the stale first Accepted decision never governs.
        assert_eq!(dispatcher.call_count(), 2);
        match outcome {
            SubmitCommandOutcome::Rejected {
                kind,
                matching_events,
                ..
            } => {
                assert_eq!(kind, "already_shipped");
                // Codeberg issue #7: the *final*, post-redispatch matching
                // set - the concurrent writer's own event - not the
                // caller's stale, empty pre-lock one.
                assert_eq!(matching_events.len(), 1);
                assert_eq!(matching_events[0].tags[0].value.as_deref(), Some("A"));
            }
            SubmitCommandOutcome::Accepted { .. } => {
                panic!("a genuine DCB conflict was not caught - stale decision was used")
            }
            SubmitCommandOutcome::Deduplicated { .. } => {
                panic!("no idempotency_key was given - Deduplicated must be unreachable")
            }
        }

        // Nothing this submission tried to write landed - only the
        // concurrent writer's own event (sequence 0) exists.
        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 0);
    });
}

#[test]
fn submit_command_does_not_redispatch_for_an_unrelated_concurrent_event() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let payload = r#"{"order_id":"A"}"#;
        let consistency_tags = skilj_core::event_store::derive_tags(&ct.tag_mappings, payload);
        let bounded_context_events: Vec<Event> = Vec::new();
        let initial_decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();

        // A concurrent write that ships a *different* order - matches no
        // tag this submission cares about, so it must not trigger a
        // redispatch at all.
        insert_concurrent_event(&pool, &bc, &et, "B").await;

        let outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            payload,
            "client-1",
            &bounded_context_events,
            &consistency_tags,
            // Caller's own pre-lock matching_events - empty, matching
            // bounded_context_events above.
            &[],
            initial_decision,
            None,
            test_now(),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            dispatcher.call_count(),
            1,
            "an unrelated concurrent event must not cost a redispatch"
        );
        assert!(matches!(outcome, SubmitCommandOutcome::Accepted { .. }));

        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        // The concurrent "B" event (sequence 0) plus this submission's
        // own "A" event (sequence 1) - gapless, in commit order.
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].sequence, 1);
    });
}

/// Drift audit finding #12 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): `Command.id` (real now, replacing
/// whole-struct equality as `fetch_commands`' own identity) round-trips
/// through a real insert - not just constructible in memory, the way the
/// pure-function tests in `command_processing.rs`/`command_query.rs`
/// alone would prove.
#[test]
fn submit_command_persists_a_real_command_id_that_round_trips() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let payload = r#"{"order_id":"A"}"#;
        let initial_decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();

        let outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            payload,
            "client-1",
            &[],
            &[],
            &[],
            initial_decision,
            None,
            test_now(),
            None,
            None,
        )
        .await
        .unwrap();

        let SubmitCommandOutcome::Accepted { command, events } = outcome else {
            panic!("this submission has no reason to be rejected");
        };
        assert!(!command.id.is_empty());
        // Every triggered event's own origin embeds the exact same id -
        // the same value fetch_commands' own triggered_event lookup now
        // compares against.
        for event in &events {
            match &event.origin {
                EventOrigin::CommandTriggered { command: origin } => {
                    assert_eq!(origin.id, command.id);
                }
                _ => panic!("every event submit_command produces here is command-triggered"),
            }
        }

        // Re-fetched from a fresh query, not the in-memory value this
        // call already returned - proves the id actually persisted, not
        // just that the domain struct in hand still carries what it was
        // built with.
        let reloaded = db::list_commands_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded[0].id, command.id);
    });
}

/// Codeberg issue #12: the actual idempotency short-circuit. Mirrors a
/// real retry - the caller re-runs its own optimistic `dispatch()` a
/// second time too (as it genuinely would on a network-timeout retry),
/// same `idempotency_key` both times. The second `submit_command` call
/// must return the *first* call's own outcome verbatim, not a fresh
/// decision, and must not insert a second `Command`/set of events.
#[test]
fn submit_command_with_a_repeated_idempotency_key_short_circuits_to_the_original_outcome() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let payload = r#"{"order_id":"A"}"#;
        let key = "retry-key-1";

        let first_decision = dispatcher.dispatch(&bc.name, &ct.name, payload, &[]).unwrap().unwrap();
        let first_outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            payload,
            "client-1",
            &[],
            &[],
            &[],
            first_decision,
            None,
            test_now(),
            None,
            Some(key),
        )
        .await
        .unwrap();
        let SubmitCommandOutcome::Accepted {
            events: first_events,
            ..
        } = first_outcome
        else {
            panic!("this submission has no reason to be rejected");
        };
        let first_sequences: Vec<i64> = first_events.iter().map(|e| e.sequence).collect();

        let second_decision = dispatcher.dispatch(&bc.name, &ct.name, payload, &[]).unwrap().unwrap();
        let second_outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            payload,
            "client-1",
            &[],
            &[],
            &[],
            second_decision,
            None,
            test_now(),
            None,
            Some(key),
        )
        .await
        .unwrap();

        let SubmitCommandOutcome::Deduplicated {
            triggered_event_sequences,
        } = second_outcome
        else {
            panic!("a repeated idempotency key must short-circuit to Deduplicated, got {second_outcome:?}");
        };
        assert_eq!(triggered_event_sequences, first_sequences);

        // Only one Command actually exists - the second call inserted
        // nothing, it just read the first call's own stored answer back.
        let commands = db::list_commands_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(
            commands.len(),
            1,
            "a dedup hit must not insert a second Command row"
        );
    });
}

/// The direct counterpart to the test above: no `idempotency_key` at all
/// (the default for every existing caller) must show today's unchanged
/// double-processing behaviour - two full `Accepted` outcomes, two real
/// `Command` rows, two distinct sets of event sequences. Proves the
/// "byte-identical when absent" claim for real, not by inspection.
#[test]
fn submit_command_without_an_idempotency_key_still_double_processes_a_repeated_submission() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let payload = r#"{"order_id":"A"}"#;

        for _ in 0..2 {
            let decision = dispatcher
                .dispatch(&bc.name, &ct.name, payload, &[])
                .unwrap()
                .unwrap();
            let outcome = db::submit_command(
                &pool,
                &dispatcher,
                &TestProjectionDispatcher,
                &broadcaster,
                &event_cache,
                &ct,
                payload,
                "client-1",
                &[],
                &[],
                &[],
                decision,
                None,
                test_now(),
                None,
                None,
            )
            .await
            .unwrap();
            assert!(
                matches!(outcome, SubmitCommandOutcome::Accepted { .. }),
                "no key given - every submission is new, exactly as before this feature existed"
            );
        }

        let commands = db::list_commands_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(
            commands.len(),
            2,
            "two keyless submissions must both persist"
        );
    });
}

/// Codeberg issue #12's migration-gap fix: `ensure_idempotency_keys_table`
/// is what `SkiljBuilder::build()`'s own per-bounded-context startup loop
/// calls unconditionally, every startup, precisely because
/// `provision_bounded_context_schema` only ever runs once, at creation -
/// a bounded context created before this feature existed would otherwise
/// never get the table. Simulates that exact "provisioned before this
/// feature existed" state by dropping the table a normal `provision`
/// already created, then proves the patch doesn't just recreate the
/// table but that idempotency actually works normally afterward.
#[test]
fn ensure_idempotency_keys_table_patches_a_bounded_context_provisioned_before_this_feature() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;

        let schema = format!("\"bc_{}\"", bc.name);
        sqlx::query(&format!("DROP TABLE {schema}.idempotency_keys"))
            .execute(&pool)
            .await
            .unwrap();

        // What SkiljBuilder::build()'s own startup loop does, per
        // bounded context, every time.
        db::ensure_idempotency_keys_table(&pool, &bc.name)
            .await
            .unwrap();

        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);
        let payload = r#"{"order_id":"A"}"#;
        let key = "post-patch-retry";

        let first_decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();
        db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            payload,
            "client-1",
            &[],
            &[],
            &[],
            first_decision,
            None,
            test_now(),
            None,
            Some(key),
        )
        .await
        .unwrap();

        let second_decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();
        let second_outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            payload,
            "client-1",
            &[],
            &[],
            &[],
            second_decision,
            None,
            test_now(),
            None,
            Some(key),
        )
        .await
        .unwrap();

        assert!(
            matches!(second_outcome, SubmitCommandOutcome::Deduplicated { .. }),
            "idempotency must work normally after the patch, not just leave the table present"
        );
    });
}

#[test]
fn submit_command_leaves_no_sequence_gap_when_process_command_fails() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        // Deliberately no EventType registered for "NeverRegistered" -
        // process_command's own UnregisteredEventType failure mode.
        let ct = seed_command_type(&pool, &bc, "TriggerUnregisteredEvent").await;
        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let payload = "{}";
        let initial_decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();

        let result = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            payload,
            "client-1",
            &[],
            &[],
            &[],
            initial_decision,
            None,
            test_now(),
            None,
            None,
        )
        .await;
        assert!(result.is_err());

        // The sequence submit_command provisionally allocated inside its
        // own transaction, before process_command failed, rolled back
        // with everything else - the very first real allocation still
        // starts at 0, not 1.
        assert_eq!(db::next_sequence(&pool, &bc.name).await.unwrap(), 0);
    });
}

#[test]
fn submit_command_rolls_back_the_command_and_every_event_together_when_a_later_event_fails_to_insert(
) {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let order_shipped = seed_order_shipped_event_type(&pool, &bc).await;
        let poison = seed_poison_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "TriggerTwoEvents").await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: "Poisonable".to_string(),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: vec![order_shipped, poison],
            sync: true,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();
        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let payload = "{}";
        let initial_decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();

        let result = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            payload,
            "client-1",
            &[],
            &[],
            &[],
            initial_decision,
            None,
            test_now(),
            None,
            None,
        )
        .await;
        assert!(
            result.is_err(),
            "the poison pill's Some(Err(..)) must fail the whole submission"
        );

        // Neither event landed - not even the first (OrderShipped),
        // which the poison pill only reaches *after*: the whole
        // transaction, command included, rolled back together.
        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert!(events.is_empty());
        // Both sequence numbers submit_command provisionally allocated
        // for the two events rolled back too.
        assert_eq!(db::next_sequence(&pool, &bc.name).await.unwrap(), 0);
    });
}
