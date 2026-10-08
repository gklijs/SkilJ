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
            "ShipCount" => Some(if _event.event_type.name == "OrderShipped" {
                vec![String::new()]
            } else {
                Vec::new()
            }),
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
            "ShipCount" => Some(Ok((state_json.parse::<i64>().unwrap() + 1).to_string())),
            _ => None,
        }
    }

    fn default_state(&self, _bounded_context: &str, projection_name: &str) -> Option<String> {
        match projection_name {
            "Poisonable" | "ShipCount" => Some("0".to_string()),
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
            "ShipCount" => Some(Some("order")),
            _ => None,
        }
    }

    fn team_only(
        &self,
        _bounded_context: &str,
        projection_name: &str,
    ) -> Option<Option<&'static str>> {
        match projection_name {
            "Poisonable" | "ShipCount" => Some(None),
            _ => None,
        }
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
    let url = skilj_test_support::database_url("skilj_submit_command_test").await?;
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
            correlation_id: None,
            causation_id: None,
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
            None,
            None,
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
            None,
            None,
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
            None,
            None,
            &[],
            &[],
            &[],
            initial_decision,
            None,
            test_now(),
            None,
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
            None,
            None,
            &[],
            &[],
            &[],
            first_decision,
            None,
            test_now(),
            None,
            Some(key),
            None,
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
            None,
            None,
            &[],
            &[],
            &[],
            second_decision,
            None,
            test_now(),
            None,
            Some(key),
            None,
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

/// [docs/architecture.md §37](../../docs/architecture.md#idempotency-keys-client-id-scoping): two different `client_id`s (two different
/// tenants, in the real multi-tenant shape owner-tag scoping
/// (`RoleAccessMapping`/`CommandToken`) actually supports) submitting
/// the *same* `CommandType` with the *same* idempotency-key string must
/// not collide - the real, already-shipped-since-0.0.2 bug behind this
/// fix, distinct from `CrossContextRoute`'s own narrower predictable-key
/// variant ([§36](../../docs/architecture.md#cross-context-route)). Before `client_id`-scoping, the second tenant's real
/// submission would have silently short-circuited to the first tenant's
/// own stored `triggered_event_sequences` instead of ever calling
/// `decide()`.
#[test]
fn submit_command_with_the_same_idempotency_key_from_two_different_clients_does_not_collide() {
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

        // A shared, business-derived key - exactly the kind of thing two
        // unrelated tenants could plausibly pick independently (an order
        // id, an invoice number), not a random UUID.
        let key = "invoice-2024-01";

        let tenant_a_decision = dispatcher
            .dispatch(&bc.name, &ct.name, r#"{"order_id":"A"}"#, &[])
            .unwrap()
            .unwrap();
        let tenant_a_outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            r#"{"order_id":"A"}"#,
            "tenant-a",
            None,
            None,
            &[],
            &[],
            &[],
            tenant_a_decision,
            None,
            test_now(),
            None,
            Some(key),
            None,
        )
        .await
        .unwrap();
        assert!(
            matches!(tenant_a_outcome, SubmitCommandOutcome::Accepted { .. }),
            "tenant a's first use of this key must be accepted, got {tenant_a_outcome:?}"
        );

        // Tenant B, a completely unrelated caller (own client_id), reuses
        // the exact same key string for their own, unrelated order - must
        // be decided for real, not silently deduplicated against tenant
        // A's own stored answer.
        let tenant_b_decision = dispatcher
            .dispatch(&bc.name, &ct.name, r#"{"order_id":"B"}"#, &[])
            .unwrap()
            .unwrap();
        let tenant_b_outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            r#"{"order_id":"B"}"#,
            "tenant-b",
            None,
            None,
            &[],
            &[],
            &[],
            tenant_b_decision,
            None,
            test_now(),
            None,
            Some(key),
            None,
        )
        .await
        .unwrap();
        let SubmitCommandOutcome::Accepted {
            events: tenant_b_events,
            ..
        } = tenant_b_outcome
        else {
            panic!(
                "tenant b's own, unrelated submission must be decided for real, not \
                 deduplicated against tenant a's - got {tenant_b_outcome:?}"
            );
        };
        assert_eq!(
            tenant_b_events.len(),
            1,
            "tenant b's own OrderShipped must actually have been inserted"
        );

        // Both tenants' own real submissions are independently persisted -
        // two Commands, not one dedup hit swallowing the second.
        let commands = db::list_commands_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(
            commands.len(),
            2,
            "two different clients' own submissions under the same key string \
             must both actually persist"
        );

        // Each client retrying their own key still deduplicates correctly
        // against their own prior answer, not the other's.
        let tenant_a_retry_decision = dispatcher
            .dispatch(&bc.name, &ct.name, r#"{"order_id":"A"}"#, &[])
            .unwrap()
            .unwrap();
        let tenant_a_retry_outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            r#"{"order_id":"A"}"#,
            "tenant-a",
            None,
            None,
            &[],
            &[],
            &[],
            tenant_a_retry_decision,
            None,
            test_now(),
            None,
            Some(key),
            None,
        )
        .await
        .unwrap();
        let SubmitCommandOutcome::Accepted {
            events: tenant_a_events,
            ..
        } = tenant_a_outcome
        else {
            unreachable!("checked above");
        };
        let tenant_a_sequences: Vec<i64> = tenant_a_events.iter().map(|e| e.sequence).collect();
        let SubmitCommandOutcome::Deduplicated {
            triggered_event_sequences,
        } = tenant_a_retry_outcome
        else {
            panic!(
                "tenant a retrying their own key must still deduplicate against \
                 their own answer, got {tenant_a_retry_outcome:?}"
            );
        };
        assert_eq!(
            triggered_event_sequences, tenant_a_sequences,
            "must dedup to tenant a's own sequences, never tenant b's"
        );
    });
}

/// [docs/architecture.md §37](../../docs/architecture.md#idempotency-keys-client-id-scoping): an already-provisioned bounded context's
/// `idempotency_keys` (a real table since 0.0.2) gets patched onto the
/// `client_id`-scoped shape - the security-review-driven follow-up to
/// `ensure_idempotency_keys_table_patches_a_bounded_context_provisioned_before_this_feature`
/// above, this time simulating a table that already has *rows* from
/// before this fix shipped, not just an absent table. Proves the
/// migration is safe to run against a populated table, and that a
/// pre-migration row is genuinely retired rather than merely reshuffled -
/// the user's own explicit call (over preserving it as a fallback for
/// whoever retries it first) to fully close the cross-tenant collision
/// class this fix exists for, accepting in exchange that a *genuine*
/// retry of a pre-migration submission arriving after this migration
/// runs won't be recognised as a duplicate. See
/// `migrate_idempotency_keys_client_id_scoping`'s own doc comment for
/// the full tradeoff.
#[test]
fn migrate_idempotency_keys_client_id_scoping_retires_pre_migration_rows() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;

        // Roll the table back to its pre-fix, 0.0.2-era shape and insert
        // a row the way that era's own `insert_idempotency_key` would
        // have - no `client_id` column at all.
        let schema = format!("\"bc_{}\"", bc.name);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP TABLE {schema}.idempotency_keys"
        )))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {schema}.idempotency_keys (
                command_type_name TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                triggered_event_sequences BIGINT[] NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (command_type_name, idempotency_key)
            )"
        )))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.idempotency_keys \
             (command_type_name, idempotency_key, triggered_event_sequences, created_at) \
             VALUES ($1, $2, $3, $4)"
        )))
        .bind(&ct.name)
        .bind("legacy-key")
        .bind([42_i64].as_slice())
        .bind(test_now())
        .execute(&pool)
        .await
        .unwrap();

        // What SkiljBuilder::build()'s own startup loop does, per
        // bounded context, every time - in the real order, since
        // `ensure_idempotency_keys_table` must run first (it's a no-op
        // here, the table already exists) before the migration checks
        // for what it might need to patch.
        db::ensure_idempotency_keys_table(&pool, &bc.name)
            .await
            .unwrap();
        db::migrate_idempotency_keys_client_id_scoping(&pool, &bc.name)
            .await
            .unwrap();

        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);
        let payload = r#"{"order_id":"A"}"#;

        // Even the caller who originally wrote the legacy row (were they
        // to retry with the exact same key string) gets a fresh,
        // real Accepted outcome, not Deduplicated - the pre-migration
        // row is never matched by anyone again, on purpose, by the
        // user's own explicit choice (see this test's own doc comment).
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
            "whoever-originally-submitted-this",
            None,
            None,
            &[],
            &[],
            &[],
            decision,
            None,
            test_now(),
            None,
            Some("legacy-key"),
            None,
        )
        .await
        .unwrap();
        let SubmitCommandOutcome::Accepted {
            events: retried_events,
            ..
        } = outcome
        else {
            panic!(
                "a pre-migration row must never be matched again - decide() runs for \
                 real, got {outcome:?}"
            );
        };
        assert_eq!(
            retried_events.len(),
            1,
            "the retry actually persisted a real new event, not a cached answer"
        );

        // The row itself is untouched, not deleted - this codebase's own
        // "nothing is ever deleted" precedent still holds; it's simply
        // never looked up again.
        let legacy_row_still_present: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT triggered_event_sequences[1] FROM {schema}.idempotency_keys \
             WHERE client_id = '' AND idempotency_key = 'legacy-key'"
        )))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(legacy_row_still_present.0, 42);

        // A different client using a key string that was never claimed
        // pre-migration gets decided for real too - ordinary, unaffected
        // behaviour for any key that isn't a pre-migration leftover.
        let fresh_decision = dispatcher
            .dispatch(&bc.name, &ct.name, r#"{"order_id":"B"}"#, &[])
            .unwrap()
            .unwrap();
        let fresh_outcome = db::submit_command(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            &broadcaster,
            &event_cache,
            &ct,
            r#"{"order_id":"B"}"#,
            "a-different-client",
            None,
            None,
            &[],
            &[],
            &[],
            fresh_decision,
            None,
            test_now(),
            None,
            Some("a-different-key-this-client-owns"),
            None,
        )
        .await
        .unwrap();
        assert!(
            matches!(fresh_outcome, SubmitCommandOutcome::Accepted { .. }),
            "a different client's own, never-before-seen key must be decided for real, \
             got {fresh_outcome:?}"
        );
    });
}

/// [docs/architecture.md §37](../../docs/architecture.md#idempotency-keys-client-id-scoping): a real fleet runs more than one skilj
/// instance, which could race this same migration against the same
/// shared Postgres at startup - untested by the migration test above,
/// which only ever calls it from one caller at a time. Genuinely races
/// several concurrent callers (`tokio::spawn`, each its own task, this
/// test's own runtime is the standard multi-threaded one - a real race,
/// not just interleaved awaits on a single thread) against the same
/// pre-migration table, and proves every one of them succeeds (no
/// error - confirmed separately, by testing, that this is actually true
/// even with `migrate_idempotency_keys_client_id_scoping`'s own
/// `pg_advisory_xact_lock` removed, since `DROP CONSTRAINT IF EXISTS`
/// plus Postgres's own whole-transaction table locking already make the
/// raw ALTER sequence race-safe on their own - see that function's own
/// doc comment for the lock's real, more modest purpose) and the final
/// shape is correct exactly once, not corrupted or double-applied.
#[test]
fn migrate_idempotency_keys_client_id_scoping_is_safe_under_concurrent_callers() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;

        // Roll the table back to its pre-fix shape, same as the test
        // above.
        let schema = format!("\"bc_{}\"", bc.name);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP TABLE {schema}.idempotency_keys"
        )))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {schema}.idempotency_keys (
                command_type_name TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                triggered_event_sequences BIGINT[] NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (command_type_name, idempotency_key)
            )"
        )))
        .execute(&pool)
        .await
        .unwrap();

        // A real fleet's own shape: several instances calling this at
        // once against the same shared Postgres, each its own spawned
        // task (and, since `Pool` is a real connection pool, plausibly
        // its own physical connection) - a genuine race, not merely
        // this function being called several times in a row.
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let pool = pool.clone();
                let name = bc.name.clone();
                tokio::spawn(async move {
                    db::migrate_idempotency_keys_client_id_scoping(&pool, &name).await
                })
            })
            .collect();

        for handle in handles {
            handle
                .await
                .expect("task panicked")
                .expect("the advisory lock must serialise concurrent migrators, not error");
        }

        // Exactly the final shape, once - not corrupted, not double-applied.
        let pk_columns: Vec<(String,)> = sqlx::query_as(
            "SELECT column_name FROM information_schema.key_column_usage \
             WHERE table_schema = $1 AND table_name = 'idempotency_keys' \
               AND constraint_name = 'idempotency_keys_pkey' \
             ORDER BY ordinal_position",
        )
        .bind(format!("bc_{}", bc.name))
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            pk_columns,
            vec![
                ("command_type_name".to_string(),),
                ("client_id".to_string(),),
                ("idempotency_key".to_string(),),
            ],
            "the primary key must end up with exactly these three columns, in this \
             order, regardless of how many concurrent callers raced to get there"
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
                None,
                None,
                &[],
                &[],
                &[],
                decision,
                None,
                test_now(),
                None,
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
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP TABLE {schema}.idempotency_keys"
        )))
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
            None,
            None,
            &[],
            &[],
            &[],
            first_decision,
            None,
            test_now(),
            None,
            Some(key),
            None,
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
            None,
            None,
            &[],
            &[],
            &[],
            second_decision,
            None,
            test_now(),
            None,
            Some(key),
            None,
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
            None,
            None,
            &[],
            &[],
            &[],
            initial_decision,
            None,
            test_now(),
            None,
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
            None,
            None,
            &[],
            &[],
            &[],
            initial_decision,
            None,
            test_now(),
            None,
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

#[test]
fn submit_command_batches_sequence_allocation_for_a_multi_event_command() {
    // Codeberg issue #32: `submit_command` claims every sequence number a
    // multi-event `CommandDecision::Accepted` needs in one
    // `next_sequence_batch` round trip rather than one `next_sequence`
    // call per event. Same `TriggerTwoEvents` decider as the poison-pill
    // rollback test above, but with no `Poisonable` projection
    // registered - `PoisonEvent`'s own `project()` handler only poisons
    // that one specific projection name, so with nothing consuming it
    // this is an ordinary two-event success, and the real thing this
    // test exists to prove is that the two resulting sequences are
    // contiguous, ascending, and assigned in `event_specs` order -
    // exactly what one batched `UPDATE ... SET next_value = next_value +
    // 2 RETURNING next_value` derives its range from.
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        seed_poison_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "TriggerTwoEvents").await;
        let dispatcher = TestCommandDispatcher::new();
        let broadcaster = EventBroadcaster::new(16);
        let event_cache = EventCache::new(1000);

        let payload = "{}";
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
            None,
            None,
            &[],
            &[],
            &[],
            initial_decision,
            None,
            test_now(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let SubmitCommandOutcome::Accepted { events, .. } = outcome else {
            panic!("no Poisonable projection is registered, so nothing rejects this");
        };
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type.name, "OrderShipped");
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].event_type.name, "PoisonEvent");
        assert_eq!(events[1].sequence, 1);

        // The batch allocation's own high-water mark left the sequence
        // row exactly where two individual `next_sequence` calls would
        // have - the next real allocation continues right after it, no
        // gap and no overlap.
        assert_eq!(db::next_sequence(&pool, &bc.name).await.unwrap(), 2);
    });
}

/// A `BatchedCommand` sharing every batch-wide collaborator this test's
/// batch calls need - only the fields that vary per command in these
/// tests are exposed as parameters, the rest fixed to what `TestCommandDispatcher`'s
/// `ShipOrder`/`TriggerTwoEvents` fixtures expect.
fn batched(
    command_type: &CommandType,
    payload: &str,
    initial_decision: CommandDecision,
    consistency_tags: Vec<Tag>,
) -> db::BatchedCommand {
    db::BatchedCommand {
        command_type: command_type.clone(),
        payload: payload.to_string(),
        client_id: "client-1".to_string(),
        correlation_id: None,
        causation_id: None,
        bounded_context_events: Vec::new(),
        covered_through: None,
        consistency_tags,
        matching_events: Vec::new(),
        initial_decision,
        now: test_now(),
        snapshot: None,
        idempotency_key: None,
        event_types_by_name: std::collections::HashMap::new(),
        resolved: std::collections::HashMap::new(),
    }
}

#[test]
fn submit_command_batch_detects_a_dcb_conflict_between_two_commands_in_the_same_batch() {
    // Codeberg issue #32, round two: `db::submit_command_batch` lets two
    // *different* commands share one lock acquisition. The DB-committed-
    // delta half of the DCB conflict check (`locked_highest >
    // original_highest`) alone cannot catch a conflict *within* the same
    // batch - both commands here start from `bounded_context_events:
    // vec![]` (`original_highest = -1`), exactly what two real
    // concurrent callers each independently deciding "nothing exists yet
    // for this order" would have computed before ever joining a batch -
    // and a freshly provisioned bounded context's own `sequence.next_value`
    // starts at `-1` too, so `locked_highest (-1) > original_highest
    // (-1)` is false for *both* commands: the pre-existing delta-fetch
    // branch never even runs. Only `extra_committed_events` - the second
    // command in the batch seeing the first one's own freshly-inserted-
    // but-not-yet-committed `OrderShipped` event - can catch this. If
    // that wiring were missing or broken, both commands would be
    // accepted, silently double-shipping the same order.
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        let dispatcher = TestCommandDispatcher::new();

        let payload = r#"{"order_id":"A"}"#;
        let tags = vec![Tag {
            key: "order".to_string(),
            value: Some("A".to_string()),
        }];
        // Each command's own initial decision, computed exactly the way
        // a real concurrent caller would - optimistically, against no
        // matching_events, before either has any idea the other exists.
        let decision_1 = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();
        let decision_2 = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();
        assert_eq!(dispatcher.call_count(), 2);

        let batch = vec![
            batched(&ct, payload, decision_1, tags.clone()),
            batched(&ct, payload, decision_2, tags),
        ];

        let mut results = db::submit_command_batch(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            None,
            &bc.name,
            batch,
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 2);

        let second = results.pop().unwrap().unwrap();
        let first = results.pop().unwrap().unwrap();

        match first {
            SubmitCommandOutcome::Accepted { events, .. } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].sequence, 0);
            }
            other => panic!("expected the first command to ship the order, got {other:?}"),
        }
        match second {
            SubmitCommandOutcome::Rejected { reason, .. } => {
                assert_eq!(reason, "already shipped");
            }
            other => panic!(
                "expected the second command to be rejected as a same-batch DCB conflict, got \
                 {other:?}"
            ),
        }

        // The redispatch this test exists to prove happened - not just
        // that the final outcome happens to look right. Two initial
        // calls (above) plus exactly one redispatch, for the second
        // command only (the first never conflicts with anything).
        assert_eq!(dispatcher.call_count(), 3);

        // Only one event ever landed - the rejected command produced
        // nothing.
        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
    });
}

#[test]
fn submit_command_batch_isolates_a_failing_commands_savepoint_from_its_batch_mates() {
    // A real per-command failure (the poison-pill projection, same
    // mechanism `submit_command_rolls_back_the_command_and_every_event_together_when_a_later_event_fails_to_insert`
    // above uses) must roll back only *that* command's own work - via
    // its own `SAVEPOINT` - not the whole batch's, and must not corrupt
    // sequence allocation for the commands after it either.
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        seed_poison_event_type(&pool, &bc).await;
        let ship_order = seed_command_type(&pool, &bc, "ShipOrder").await;
        let trigger_two = seed_command_type(&pool, &bc, "TriggerTwoEvents").await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: "Poisonable".to_string(),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: Vec::new(),
            sync: true,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();
        let dispatcher = TestCommandDispatcher::new();

        let ship_payload = r#"{"order_id":"A"}"#;
        let ship_decision = dispatcher
            .dispatch(&bc.name, &ship_order.name, ship_payload, &[])
            .unwrap()
            .unwrap();
        let poison_decision = dispatcher
            .dispatch(&bc.name, &trigger_two.name, "{}", &[])
            .unwrap()
            .unwrap();

        let batch = vec![
            batched(
                &ship_order,
                ship_payload,
                ship_decision,
                vec![Tag {
                    key: "order".to_string(),
                    value: Some("A".to_string()),
                }],
            ),
            batched(&trigger_two, "{}", poison_decision, Vec::new()),
        ];

        let mut results = db::submit_command_batch(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            None,
            &bc.name,
            batch,
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 2);

        let second = results.pop().unwrap();
        let first = results.pop().unwrap();

        match first.unwrap() {
            SubmitCommandOutcome::Accepted { events, .. } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].sequence, 0);
            }
            other => panic!("expected the first command to succeed, got {other:?}"),
        }
        assert!(
            second.is_err(),
            "the poison pill's Some(Err(..)) must fail only its own command"
        );

        // Only the first command's own event survived - the second
        // command's own savepoint rolled back both its events (the
        // poison pill fails on the *second* one, but the whole command's
        // savepoint rolls back together, same as the standalone
        // `submit_command` rollback test).
        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type.name, "OrderShipped");

        // The second command's own provisional sequence allocation
        // (`next_sequence_batch`, for its two events) rolled back with
        // its savepoint too - the next real allocation continues right
        // after the first command's own single event, no gap, no
        // overlap, and no sequence burned on a command that never
        // landed.
        assert_eq!(db::next_sequence(&pool, &bc.name).await.unwrap(), 1);
    });
}

#[test]
fn submit_command_batch_recycles_a_failed_commands_sequence_numbers_for_a_later_command_in_the_same_batch(
) {
    // Codeberg issue #32, round three: `commit_command_batch` now pools
    // sequence-number reservations across the whole batch (one bigger
    // `next_sequence_batch` call instead of one per command) rather than
    // allocating strictly per command inside each one's own `SAVEPOINT`.
    // The real risk that pooling introduces is a permanent gap: if a
    // command draws numbers from the shared pool and then fails, those
    // numbers must not simply be lost - they need to end up used by
    // *some* successfully-committed event, or corrected back out of
    // `{schema}.sequence` entirely, never left allocated-but-orphaned.
    //
    // Three commands, same shape as the "isolates" test above but with a
    // *third*, unrelated command after the failing one: ShipOrder("A")
    // succeeds (1 event), TriggerTwoEvents fails on its poison pill
    // (would have needed 2), ShipOrder("B") succeeds (1 event). If the
    // pool's own failed-draw recycling works, ShipOrder("B")'s event
    // lands on one of the two numbers TriggerTwoEvents drew and then
    // returned - sequence 1, not a fresh number past the reservation
    // TriggerTwoEvents already made. If recycling were missing or
    // broken (numbers simply abandoned on failure), either ShipOrder("B")
    // would get a higher, non-recycled number while a lower one sits
    // forever unused (a real gap `SequenceIsGaplessPerBoundedContext`
    // forbids), or the final `next_sequence` would land somewhere other
    // than the two real events actually justify.
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        seed_poison_event_type(&pool, &bc).await;
        let ship_order = seed_command_type(&pool, &bc, "ShipOrder").await;
        let trigger_two = seed_command_type(&pool, &bc, "TriggerTwoEvents").await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: "Poisonable".to_string(),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: Vec::new(),
            sync: true,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();
        let dispatcher = TestCommandDispatcher::new();

        let ship_a_payload = r#"{"order_id":"A"}"#;
        let ship_a_decision = dispatcher
            .dispatch(&bc.name, &ship_order.name, ship_a_payload, &[])
            .unwrap()
            .unwrap();
        let poison_decision = dispatcher
            .dispatch(&bc.name, &trigger_two.name, "{}", &[])
            .unwrap()
            .unwrap();
        let ship_b_payload = r#"{"order_id":"B"}"#;
        let ship_b_decision = dispatcher
            .dispatch(&bc.name, &ship_order.name, ship_b_payload, &[])
            .unwrap()
            .unwrap();

        let batch = vec![
            batched(
                &ship_order,
                ship_a_payload,
                ship_a_decision,
                vec![Tag {
                    key: "order".to_string(),
                    value: Some("A".to_string()),
                }],
            ),
            batched(&trigger_two, "{}", poison_decision, Vec::new()),
            batched(
                &ship_order,
                ship_b_payload,
                ship_b_decision,
                vec![Tag {
                    key: "order".to_string(),
                    value: Some("B".to_string()),
                }],
            ),
        ];

        let mut results = db::submit_command_batch(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            None,
            &bc.name,
            batch,
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 3);

        let third = results.pop().unwrap();
        let second = results.pop().unwrap();
        let first = results.pop().unwrap();

        match first.unwrap() {
            SubmitCommandOutcome::Accepted { events, .. } => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].sequence, 0);
            }
            other => panic!("expected the first command to succeed, got {other:?}"),
        }
        assert!(
            second.is_err(),
            "the poison pill's Some(Err(..)) must fail only its own command"
        );
        match third.unwrap() {
            SubmitCommandOutcome::Accepted { events, .. } => {
                assert_eq!(events.len(), 1);
                assert_eq!(
                    events[0].sequence, 1,
                    "the third command's own event must land on the sequence number the \
                     failed second command drew and gave back, not a fresh one past it - \
                     otherwise sequence 1 would be a permanent gap"
                );
            }
            other => panic!("expected the third command to succeed, got {other:?}"),
        }

        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        // Exactly the two real events justify this - nothing reserved
        // and never used was left dangling in `{schema}.sequence`.
        assert_eq!(db::next_sequence(&pool, &bc.name).await.unwrap(), 2);
    });
}

#[test]
fn submit_command_batch_deduplicates_a_repeated_idempotency_key_shared_by_two_commands_in_the_same_batch(
) {
    // The idempotency lookup (`decide_command_in_tx`'s own
    // `lookup_idempotency_key` call) still runs against the batch
    // leader's own `tx`, not `pool` - unchanged by the round-three split
    // into `decide_command_in_tx`/`finish_accepted_command_in_tx` - so it
    // must still see an *earlier command in this same batch*'s own
    // idempotency-key insert, uncommitted but already visible on `tx`
    // via that command's own released `SAVEPOINT`, exactly as it did
    // before that split. If this regressed (say, the lookup silently
    // moved to `pool`, a different connection that can't see `tx`'s
    // uncommitted state), two concurrent callers retrying with the same
    // idempotency key that happened to land in the same batch would both
    // ship the order instead of the second one deduplicating.
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        let dispatcher = TestCommandDispatcher::new();

        let payload = r#"{"order_id":"A"}"#;
        let decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();

        let mut first = batched(
            &ct,
            payload,
            decision.clone(),
            vec![Tag {
                key: "order".to_string(),
                value: Some("A".to_string()),
            }],
        );
        first.idempotency_key = Some("retry-key".to_string());
        let mut second = batched(
            &ct,
            payload,
            decision,
            vec![Tag {
                key: "order".to_string(),
                value: Some("A".to_string()),
            }],
        );
        second.idempotency_key = Some("retry-key".to_string());

        let mut results = db::submit_command_batch(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            None,
            &bc.name,
            vec![first, second],
        )
        .await
        .unwrap();
        assert_eq!(results.len(), 2);

        let second = results.pop().unwrap().unwrap();
        let first = results.pop().unwrap().unwrap();

        let first_sequence = match first {
            SubmitCommandOutcome::Accepted { events, .. } => {
                assert_eq!(events.len(), 1);
                events[0].sequence
            }
            other => panic!("expected the first command to ship the order, got {other:?}"),
        };
        match second {
            SubmitCommandOutcome::Deduplicated {
                triggered_event_sequences,
            } => {
                assert_eq!(
                    triggered_event_sequences,
                    vec![first_sequence],
                    "the second command's own idempotency lookup must see the first \
                     command's already-inserted (if not yet durably committed) key"
                );
            }
            other => panic!("expected the second, same-key command to deduplicate, got {other:?}"),
        }

        // Only one event ever landed - the deduplicated command produced
        // nothing new.
        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
    });
}

/// docs/architecture.md §178: with the position the optimistic read is
/// complete through, the re-check under the lock starts there - and still
/// catches a conflicting event committed after it. Two unrelated events
/// move the history on first, so the read for order "A" is complete
/// through sequence 1 while holding nothing.
#[test]
fn a_known_read_position_still_catches_a_conflict_committed_after_it() {
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

        insert_concurrent_event(&pool, &bc, &et, "X").await;
        insert_concurrent_event(&pool, &bc, &et, "Y").await;
        let payload = r#"{"order_id":"A"}"#;
        let consistency_tags = skilj_core::event_store::derive_tags(&ct.tag_mappings, payload);
        let initial_decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &[])
            .unwrap()
            .unwrap();
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
            None,
            None,
            &[],
            &consistency_tags,
            &[],
            initial_decision,
            None,
            test_now(),
            None,
            None,
            Some(1),
        )
        .await
        .unwrap();

        assert_eq!(dispatcher.call_count(), 2, "redispatched");
        assert!(
            matches!(outcome, SubmitCommandOutcome::Rejected { ref matching_events, .. } if matching_events.len() == 1),
            "the conflict after the read's position was missed: {outcome:?}"
        );
    });
}

/// docs/architecture.md §178: a read from Postgres can hold an event above
/// the position it reports (it saw a commit made after that position was
/// read). The re-check starts above the highest event the read holds, so
/// that event doesn't come back as new - no redispatch, counted once.
#[test]
fn an_event_the_read_already_holds_above_its_position_is_not_counted_twice() {
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

        insert_concurrent_event(&pool, &bc, &et, "A").await;
        let held = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        let payload = r#"{"order_id":"A"}"#;
        let consistency_tags = skilj_core::event_store::derive_tags(&ct.tag_mappings, payload);
        let initial_decision = dispatcher
            .dispatch(&bc.name, &ct.name, payload, &held)
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
            None,
            None,
            &held,
            &consistency_tags,
            &held,
            initial_decision,
            None,
            test_now(),
            None,
            None,
            // Read before the event committed, the read itself after.
            Some(-1),
        )
        .await
        .unwrap();

        assert_eq!(dispatcher.call_count(), 1, "nothing new - no redispatch");
        assert!(
            matches!(outcome, SubmitCommandOutcome::Rejected { ref matching_events, .. } if matching_events.len() == 1),
            "the held event must count once: {outcome:?}"
        );
    });
}

/// docs/architecture.md §196: a batch is written as a set - its events
/// numbered in memory and inserted together with their commands and
/// idempotency keys. Every event names the command that triggered it, the
/// sequence moves once by exactly what was used, and a key recorded this
/// way is found by a later batch.
#[test]
fn a_batch_written_as_a_set_links_its_events_and_records_its_keys() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        let dispatcher = TestCommandDispatcher::new();
        let command = |order: &str, key: Option<&str>| {
            let payload = format!(r#"{{"order_id":"{order}"}}"#);
            let decision = dispatcher
                .dispatch(&bc.name, &ct.name, &payload, &[])
                .unwrap()
                .unwrap();
            let mut item = batched(
                &ct,
                &payload,
                decision,
                vec![Tag {
                    key: "order".to_string(),
                    value: Some(order.to_string()),
                }],
            );
            item.idempotency_key = key.map(str::to_string);
            item
        };

        let results = db::submit_command_batch(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            None,
            &bc.name,
            vec![
                command("B", Some("key-b")),
                command("C", None),
                command("D", None),
            ],
        )
        .await
        .unwrap();
        let mut accepted = Vec::new();
        for result in results {
            match result.unwrap() {
                SubmitCommandOutcome::Accepted { command, events } => {
                    assert_eq!(events.len(), 1);
                    accepted.push((command.id.clone(), events[0].sequence));
                }
                other => panic!("expected every command to be accepted, got {other:?}"),
            }
        }
        assert_eq!(
            accepted.iter().map(|(_, s)| *s).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(db::next_sequence(&pool, &bc.name).await.unwrap(), 3);

        let events = db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .unwrap();
        for (event, (command_id, sequence)) in events.iter().zip(&accepted) {
            assert_eq!(event.sequence, *sequence);
            match &event.origin {
                EventOrigin::CommandTriggered { command } => {
                    assert_eq!(&command.id, command_id, "event {sequence}")
                }
                other => panic!("expected a command-triggered event, got {other:?}"),
            }
        }

        let results = db::submit_command_batch(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            None,
            &bc.name,
            vec![command("B", Some("key-b"))],
        )
        .await
        .unwrap();
        match results.into_iter().next().unwrap().unwrap() {
            SubmitCommandOutcome::Deduplicated {
                triggered_event_sequences,
            } => assert_eq!(triggered_event_sequences, vec![0]),
            other => panic!("expected the repeated key to deduplicate, got {other:?}"),
        }
    });
}

/// docs/architecture.md §196: the batch re-checks every stale read with
/// one query over all their tags, and each command keeps only its own
/// part. Orders "A" and "C" ship concurrently before the batch locks; of
/// three commands read before that, the ones for "A" and "C" see their
/// own conflict and are rejected with it alone, and "B", whose tag the
/// shared query also covered, is accepted.
#[test]
fn one_recheck_per_batch_gives_each_command_only_its_own_conflicts() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        let dispatcher = TestCommandDispatcher::new();
        let command = |order: &str| {
            let payload = format!(r#"{{"order_id":"{order}"}}"#);
            let decision = dispatcher
                .dispatch(&bc.name, &ct.name, &payload, &[])
                .unwrap()
                .unwrap();
            batched(
                &ct,
                &payload,
                decision,
                vec![Tag {
                    key: "order".to_string(),
                    value: Some(order.to_string()),
                }],
            )
        };
        let batch = vec![command("A"), command("B"), command("C")];
        insert_concurrent_event(&pool, &bc, &et, "A").await;
        insert_concurrent_event(&pool, &bc, &et, "C").await;

        let results = db::submit_command_batch(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            None,
            &bc.name,
            batch,
        )
        .await
        .unwrap();
        let outcomes: Vec<_> = results.into_iter().map(Result::unwrap).collect();
        for (outcome, order) in outcomes.iter().zip(["A", "B", "C"]) {
            match (order, outcome) {
                ("B", SubmitCommandOutcome::Accepted { events, .. }) => {
                    assert_eq!(events[0].sequence, 2)
                }
                (
                    _,
                    SubmitCommandOutcome::Rejected {
                        matching_events, ..
                    },
                ) if order != "B" => {
                    assert_eq!(matching_events.len(), 1, "{order}");
                    assert_eq!(matching_events[0].tags[0].value.as_deref(), Some(order));
                }
                (order, other) => panic!("unexpected outcome for {order}: {other:?}"),
            }
        }
    });
}

/// docs/architecture.md §196: a batch folds its events into a sync
/// projection at once - one row read and locked, the events folded in
/// order, written back once. Three orders ship in one batch into one
/// counting row: it counts three, takes the last event's owner and
/// position, and the projection is caught up to the last event.
#[test]
fn a_batch_folds_its_events_into_a_sync_projection_at_once() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_order_shipped_event_type(&pool, &bc).await;
        let ct = seed_command_type(&pool, &bc, "ShipOrder").await;
        db::upsert_projection(
            &pool,
            &Projection {
                bounded_context: bc.clone(),
                name: "ShipCount".to_string(),
                schema: r#"{"properties":{}}"#.to_string(),
                schema_version: 1,
                consumed_event_types: vec![et.clone()],
                sync: true,
                caught_up_to: None,
            },
        )
        .await
        .unwrap();
        let dispatcher = TestCommandDispatcher::new();
        let command = |order: &str| {
            let payload = format!(r#"{{"order_id":"{order}"}}"#);
            let decision = dispatcher
                .dispatch(&bc.name, &ct.name, &payload, &[])
                .unwrap()
                .unwrap();
            batched(
                &ct,
                &payload,
                decision,
                vec![Tag {
                    key: "order".to_string(),
                    value: Some(order.to_string()),
                }],
            )
        };

        let results = db::submit_command_batch(
            &pool,
            &dispatcher,
            &TestProjectionDispatcher,
            None,
            &bc.name,
            vec![command("B"), command("C"), command("D")],
        )
        .await
        .unwrap();
        assert!(results
            .iter()
            .all(|r| matches!(r, Ok(SubmitCommandOutcome::Accepted { .. }))));

        let (state, owner, as_of): (String, Option<String>, i64) =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT state, owner, as_of_sequence FROM \"bc_{}\".projection_state \
                 WHERE projection_name = 'ShipCount' AND key = ''",
                bc.name
            )))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            (state.as_str(), owner.as_deref(), as_of),
            ("3", Some("D"), 2)
        );
        let projection = db::get_projection(&pool, &bc.name, "ShipCount")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(projection.caught_up_to, Some(2));
    });
}
