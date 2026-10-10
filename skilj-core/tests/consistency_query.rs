//! A command's consistency query (docs/architecture.md §198): what
//! `decide()` reads, and what conflicts with it. The fixture is the
//! helpdesk's `CreateTicket`, which needs the company's latest lifecycle
//! event and whether its own ticket id was used, not every event carrying
//! the company's tag. Same `DATABASE_URL`-then-embedded-Postgres-then-skip
//! harness as `submit_command.rs`.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, CommandType, Event, EventBroadcaster, EventOrigin,
    EventType,
};
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher, SnapshotDispatcher};
use skilj_core::shared::{
    generate_token_id, CommandDecision, EventSpec, Metadata, QueryItemMapping, Tag, TagMapping,
};
use std::sync::Mutex;

fn company() -> TagMapping {
    TagMapping {
        key: "company".to_string(),
        field: "company_id".to_string(),
    }
}

fn ticket() -> TagMapping {
    TagMapping {
        key: "ticket".to_string(),
        field: "ticket_id".to_string(),
    }
}

/// `CreateTicket` decides on the company's latest lifecycle event and on
/// whether a `TicketCreated` used its ticket id - and records what each
/// call was given.
struct HelpdeskDispatcher {
    seen: Mutex<Vec<Vec<i64>>>,
}

impl HelpdeskDispatcher {
    fn new() -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<Vec<i64>> {
        self.seen.lock().unwrap().clone()
    }
}

impl CommandDispatcher for HelpdeskDispatcher {
    fn dispatch(
        &self,
        _bounded_context: &str,
        command_type: &str,
        payload: &str,
        matching_events: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        if command_type == "RecentTickets" {
            self.seen
                .lock()
                .unwrap()
                .push(matching_events.iter().map(|e| e.sequence).collect());
            return Some(Ok(CommandDecision::Accepted { events: vec![] }));
        }
        if command_type != "CreateTicket" {
            return None;
        }
        self.seen
            .lock()
            .unwrap()
            .push(matching_events.iter().map(|e| e.sequence).collect());
        let rejected = |kind: &str| {
            Some(Ok(CommandDecision::Rejected {
                reason: kind.to_string(),
                kind: kind.to_string(),
            }))
        };
        let status = matching_events
            .iter()
            .rfind(|e| e.event_type.name != "TicketCreated")
            .map(|e| e.event_type.name.as_str());
        match status {
            None => return rejected("company_not_found"),
            Some("CompanyExpired") => return rejected("company_expired"),
            Some(_) => {}
        }
        if matching_events
            .iter()
            .any(|e| e.event_type.name == "TicketCreated")
        {
            return rejected("ticket_already_exists");
        }
        Some(Ok(CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "TicketCreated".to_string(),
                payload: serde_json::from_str(payload).unwrap(),
            }],
        }))
    }

    fn required_role(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        Some(None)
    }

    fn snapshot_name(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        Some(None)
    }

    fn consistency_query(
        &self,
        _bounded_context: &str,
        command_type: &str,
    ) -> Option<Vec<QueryItemMapping>> {
        if command_type == "RecentTickets" {
            return Some(vec![QueryItemMapping::types(&["TicketCreated"])
                .tagged(vec![company()])
                .last(2)]);
        }
        (command_type == "CreateTicket").then(|| {
            vec![
                QueryItemMapping::types(&["CompanySignedUp", "CompanyExpired"])
                    .tagged(vec![company()])
                    .latest(),
                QueryItemMapping::types(&["TicketCreated"]).tagged(vec![ticket()]),
            ]
        })
    }

    fn dispatch_from_snapshot(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        unreachable!("no snapshot here")
    }
}

struct NoProjections;

impl ProjectionDispatcher for NoProjections {
    fn keys(&self, _: &str, _: &str, _: &Event) -> Option<Vec<String>> {
        None
    }

    fn project(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &Event,
        _: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }

    fn default_state(&self, _: &str, _: &str) -> Option<String> {
        None
    }

    fn owner_tag_key(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }

    fn team_only(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
}

struct NoSnapshots;

impl SnapshotDispatcher for NoSnapshots {
    fn snapshot_names(&self, _: &str) -> Vec<&'static str> {
        Vec::new()
    }

    fn tag_key(&self, _: &str, _: &str) -> Option<&'static str> {
        None
    }

    fn owner_tag_key(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }

    fn version(&self, _: &str, _: &str) -> Option<u64> {
        None
    }

    fn fold(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }

    fn default_state(&self, _: &str, _: &str) -> Option<String> {
        None
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

static TEST_DB: tokio::sync::OnceCell<Option<Pool>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio runtime"))
}

async fn test_pool() -> Option<Pool> {
    TEST_DB
        .get_or_init(|| async {
            let url = skilj_test_support::database_url("skilj_consistency_query_test").await?;
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
            Some(pool)
        })
        .await
        .clone()
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

struct Helpdesk {
    bc: BoundedContext,
    signed_up: EventType,
    expired: EventType,
    created: EventType,
    create_ticket: CommandType,
    recent_tickets: CommandType,
}

async fn seed(pool: &Pool) -> Helpdesk {
    let bc = BoundedContext {
        name: format!("bc_{}", generate_token_id()),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    let event_type = |name: &str, tag_mappings: Vec<TagMapping>| EventType {
        bounded_context: bc.clone(),
        name: name.to_string(),
        schema: r#"{"properties":{"company_id":{"type":"string"},"ticket_id":{"type":"string"}}}"#
            .to_string(),
        schema_version: 1,
        tag_mappings,
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
    let signed_up = event_type("CompanySignedUp", vec![company()]);
    let expired = event_type("CompanyExpired", vec![company()]);
    let created = event_type("TicketCreated", vec![company(), ticket()]);
    for et in [&signed_up, &expired, &created] {
        db::upsert_event_type(pool, et).await.unwrap();
    }
    let create_ticket = CommandType {
        bounded_context: bc.clone(),
        name: "CreateTicket".to_string(),
        schema: r#"{"properties":{"company_id":{"type":"string"},"ticket_id":{"type":"string"}}}"#
            .to_string(),
        schema_version: 1,
        tag_mappings: vec![company(), ticket()],
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        rest_trigger_allowed: true,
    };
    db::upsert_command_type(pool, &create_ticket).await.unwrap();
    let recent_tickets = CommandType {
        name: "RecentTickets".to_string(),
        tag_mappings: vec![company()],
        ..create_ticket.clone()
    };
    db::upsert_command_type(pool, &recent_tickets)
        .await
        .unwrap();
    Helpdesk {
        bc,
        signed_up,
        expired,
        created,
        create_ticket,
        recent_tickets,
    }
}

/// Commits an event of `et` for `company`, and `ticket` when given,
/// tagged as `et` maps them.
async fn record(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    company: &str,
    ticket: Option<&str>,
) -> i64 {
    let payload = match ticket {
        Some(ticket) => serde_json::json!({ "company_id": company, "ticket_id": ticket }),
        None => serde_json::json!({ "company_id": company }),
    }
    .to_string();
    let sequence = db::next_sequence(pool, &bc.name).await.unwrap();
    let event = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        tags: skilj_core::event_store::derive_tags(&et.tag_mappings, &payload),
        payload,
        metadata: Metadata {
            r#type: et.name.clone(),
            version: 1,
            client_id: "test".to_string(),
            created_at: test_now(),
            correlation_id: None,
            causation_id: None,
        },
        sequence,
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event(pool, &event, None).await.unwrap();
    sequence
}

async fn create_ticket(
    pool: &Pool,
    h: &Helpdesk,
    dispatcher: &HelpdeskDispatcher,
    company: &str,
    ticket: &str,
) -> SubmitCommandOutcome {
    db::decide_and_submit_command(
        pool,
        dispatcher,
        &NoProjections,
        &NoSnapshots,
        &EventBroadcaster::new(16),
        &EventCache::new(1000),
        &h.create_ticket,
        &serde_json::json!({ "company_id": company, "ticket_id": ticket }).to_string(),
        "client",
        None,
        None,
        None,
        test_now(),
        None,
    )
    .await
    .unwrap()
}

fn kind(outcome: &SubmitCommandOutcome) -> &str {
    match outcome {
        SubmitCommandOutcome::Accepted { .. } => "accepted",
        SubmitCommandOutcome::Rejected { kind, .. } => kind,
        SubmitCommandOutcome::Deduplicated { .. } => "deduplicated",
    }
}

/// `decide()` gets the company's latest lifecycle event and nothing of
/// its other tickets - the read no longer grows with them - and the
/// stored command records the query it was decided with.
#[test]
fn decide_reads_only_what_the_query_selects() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let h = seed(&pool).await;
        record(&pool, &h.bc, &h.signed_up, "acme", None).await;
        record(&pool, &h.bc, &h.expired, "acme", None).await;
        let resigned = record(&pool, &h.bc, &h.signed_up, "acme", None).await;
        for t in ["t1", "t2", "t3"] {
            record(&pool, &h.bc, &h.created, "acme", Some(t)).await;
        }
        record(&pool, &h.bc, &h.signed_up, "other", None).await;

        let dispatcher = HelpdeskDispatcher::new();
        let outcome = create_ticket(&pool, &h, &dispatcher, "acme", "t4").await;
        assert_eq!(kind(&outcome), "accepted");
        assert_eq!(dispatcher.calls(), vec![vec![resigned]]);

        let commands = db::list_commands_for_bounded_context(&pool, &h.bc.name)
            .await
            .unwrap();
        let query = &commands.last().unwrap().consistency_query;
        assert_eq!(query.len(), 2);
        assert_eq!(query[0].latest, Some(1));
        assert_eq!(
            query[1].tags,
            vec![Tag {
                key: "ticket".to_string(),
                value: Some("t4".to_string()),
            }]
        );
    });
}

/// A latest-only item gives the latest match, so an expired company
/// refuses tickets; a used ticket id is found through its own item.
#[test]
fn the_latest_lifecycle_event_and_the_ticket_id_decide() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let h = seed(&pool).await;
        let dispatcher = HelpdeskDispatcher::new();
        assert_eq!(
            kind(&create_ticket(&pool, &h, &dispatcher, "acme", "t1").await),
            "company_not_found"
        );
        record(&pool, &h.bc, &h.signed_up, "acme", None).await;
        assert_eq!(
            kind(&create_ticket(&pool, &h, &dispatcher, "acme", "t1").await),
            "accepted"
        );
        assert_eq!(
            kind(&create_ticket(&pool, &h, &dispatcher, "acme", "t1").await),
            "ticket_already_exists"
        );
        record(&pool, &h.bc, &h.expired, "acme", None).await;
        assert_eq!(
            kind(&create_ticket(&pool, &h, &dispatcher, "acme", "t2").await),
            "company_expired"
        );
    });
}

fn payload(ticket: &str) -> String {
    serde_json::json!({ "company_id": "acme", "ticket_id": ticket }).to_string()
}

/// The read before the lock, and the decision on it.
async fn resolve(
    pool: &Pool,
    h: &Helpdesk,
    dispatcher: &HelpdeskDispatcher,
    cache: &EventCache,
    ticket: &str,
) -> db::ResolvedCommandSubmission {
    db::resolve_command_submission(
        pool,
        dispatcher,
        &NoSnapshots,
        cache,
        &h.create_ticket,
        &payload(ticket),
    )
    .await
    .unwrap()
}

/// The locked half, for what `resolve` read.
async fn submit(
    pool: &Pool,
    h: &Helpdesk,
    dispatcher: &HelpdeskDispatcher,
    cache: &EventCache,
    ticket: &str,
    resolved: db::ResolvedCommandSubmission,
) -> SubmitCommandOutcome {
    db::submit_command(
        pool,
        dispatcher,
        &NoProjections,
        &EventBroadcaster::new(16),
        cache,
        &h.create_ticket,
        &payload(ticket),
        "client",
        None,
        None,
        &resolved.bounded_context_events,
        &resolved.consistency_tags,
        &resolved.matching_events,
        resolved.decision,
        None,
        test_now(),
        None,
        None,
        Some(resolved.covered_through),
    )
    .await
    .unwrap()
}

/// Between the read and the lock: another ticket of the same company
/// carries the company's tag but matches no item, so it isn't a conflict
/// and `decide()` runs once. The company expiring matches the latest-only
/// item, so it is: `decide()` runs again with the expiry in place of the
/// sign-up it read.
#[test]
fn only_an_event_matching_the_query_conflicts() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let h = seed(&pool).await;
        record(&pool, &h.bc, &h.signed_up, "acme", None).await;
        let dispatcher = HelpdeskDispatcher::new();
        let cache = EventCache::new(1000);

        let resolved = resolve(&pool, &h, &dispatcher, &cache, "t1").await;
        record(&pool, &h.bc, &h.created, "acme", Some("t0")).await;
        let outcome = submit(&pool, &h, &dispatcher, &cache, "t1", resolved).await;
        assert_eq!(kind(&outcome), "accepted");
        assert_eq!(dispatcher.calls().len(), 1, "no redispatch");

        let resolved = resolve(&pool, &h, &dispatcher, &cache, "t2").await;
        let expired = record(&pool, &h.bc, &h.expired, "acme", None).await;
        let outcome = submit(&pool, &h, &dispatcher, &cache, "t2", resolved).await;
        assert_eq!(kind(&outcome), "company_expired");
        let calls = dispatcher.calls();
        assert_eq!(calls.len(), 3, "decided again after the conflict");
        assert_eq!(calls[2], vec![expired]);
    });
}

/// A bounded context without `event_tags` and `commands.consistency_query`,
/// from before them or added by an older instance during a rolling deploy,
/// gets both from the first command a newer instance submits to it, with every existing event's tags, and the trigger indexes every
/// event after that (docs/architecture.md §200).
#[test]
fn a_bounded_context_without_event_tags_gets_them_from_its_first_command() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let h = seed(&pool).await;
        let schema = format!("\"bc_{}\"", h.bc.name);
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP TRIGGER events_index_tags ON {schema}.events; \
             DROP FUNCTION {schema}.index_event_tags(); DROP TABLE {schema}.event_tags; \
             ALTER TABLE {schema}.commands DROP COLUMN consistency_query"
        )))
        .execute(&pool)
        .await
        .unwrap();
        record(&pool, &h.bc, &h.signed_up, "acme", None).await;
        record(&pool, &h.bc, &h.created, "acme", Some("t1")).await;

        let dispatcher = HelpdeskDispatcher::new();
        assert_eq!(
            kind(&create_ticket(&pool, &h, &dispatcher, "acme", "t2").await),
            "accepted"
        );

        let rows: Vec<(String, Option<String>, String, i64)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT tag_key, tag_value, event_type_name, sequence FROM {schema}.event_tags \
                 ORDER BY sequence, tag_key"
            )))
            .fetch_all(&pool)
            .await
            .unwrap();
        let row = |key: &str, value: &str, et: &str, sequence: i64| {
            (
                key.to_string(),
                Some(value.to_string()),
                et.to_string(),
                sequence,
            )
        };
        assert_eq!(
            rows,
            vec![
                row("company", "acme", "CompanySignedUp", 0),
                row("company", "acme", "TicketCreated", 1),
                row("ticket", "t1", "TicketCreated", 1),
                row("company", "acme", "TicketCreated", 2),
                row("ticket", "t2", "TicketCreated", 2),
            ]
        );

        assert_eq!(
            kind(&create_ticket(&pool, &h, &dispatcher, "acme", "t2").await),
            "ticket_already_exists"
        );
    });
}

/// The batch re-check (docs/architecture.md §196) filters its one tag
/// query by each command's own query: three tickets of one company, read
/// before another ticket of it was created, are all accepted and none is
/// decided twice. Without the query, that ticket carries the company's
/// tag and conflicts with all three.
#[test]
fn a_batch_rechecks_each_command_against_its_own_query() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let h = seed(&pool).await;
        record(&pool, &h.bc, &h.signed_up, "acme", None).await;
        let dispatcher = HelpdeskDispatcher::new();
        let cache = EventCache::new(1000);
        let mut batch = Vec::new();
        for ticket in ["t1", "t2", "t3"] {
            let resolved = resolve(&pool, &h, &dispatcher, &cache, ticket).await;
            batch.push(db::BatchedCommand {
                command_type: h.create_ticket.clone(),
                payload: payload(ticket),
                client_id: "client".to_string(),
                correlation_id: None,
                causation_id: None,
                bounded_context_events: resolved.bounded_context_events,
                covered_through: Some(resolved.covered_through),
                consistency_tags: resolved.consistency_tags,
                matching_events: resolved.matching_events,
                initial_decision: resolved.decision,
                now: test_now(),
                snapshot: None,
                idempotency_key: None,
                event_types_by_name: std::collections::HashMap::new(),
                resolved: std::collections::HashMap::new(),
            });
        }
        record(&pool, &h.bc, &h.created, "acme", Some("t0")).await;

        let results =
            db::submit_command_batch(&pool, &dispatcher, &NoProjections, None, &h.bc.name, batch)
                .await
                .unwrap();
        for result in results {
            assert_eq!(kind(&result.unwrap()), "accepted");
        }
        assert_eq!(dispatcher.calls().len(), 3, "no command decided twice");
    });
}

/// `last(2)` hands `decide()` the company's two newest tickets, not all
/// of them. A ticket committed between read and lock still conflicts: the
/// command is decided again with it, and it displaces the older of the
/// two.
#[test]
fn an_item_keeps_its_last_n_matches() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let h = seed(&pool).await;
        let mut tickets = Vec::new();
        for t in ["t1", "t2", "t3", "t4"] {
            tickets.push(record(&pool, &h.bc, &h.created, "acme", Some(t)).await);
        }
        record(&pool, &h.bc, &h.created, "other", Some("o1")).await;
        let dispatcher = HelpdeskDispatcher::new();
        let cache = EventCache::new(1000);
        let payload = payload("x");

        let resolved = db::resolve_command_submission(
            &pool,
            &dispatcher,
            &NoSnapshots,
            &cache,
            &h.recent_tickets,
            &payload,
        )
        .await
        .unwrap();
        assert_eq!(resolved.matching_events.len(), 2);
        assert_eq!(dispatcher.calls(), vec![vec![tickets[2], tickets[3]]]);
        // The same read from Postgres, the cache off: `LIMIT 2` there.
        let uncached = db::resolve_command_submission(
            &pool,
            &dispatcher,
            &NoSnapshots,
            &EventCache::new(0),
            &h.recent_tickets,
            &payload,
        )
        .await
        .unwrap();
        let sequences: Vec<i64> = uncached
            .matching_events
            .iter()
            .map(|e| e.sequence)
            .collect();
        assert_eq!(sequences, vec![tickets[2], tickets[3]]);

        let newest = record(&pool, &h.bc, &h.created, "acme", Some("t5")).await;
        let outcome = db::submit_command(
            &pool,
            &dispatcher,
            &NoProjections,
            &EventBroadcaster::new(16),
            &cache,
            &h.recent_tickets,
            &payload,
            "client",
            None,
            None,
            &resolved.bounded_context_events,
            &resolved.consistency_tags,
            &resolved.matching_events,
            resolved.decision,
            None,
            test_now(),
            None,
            None,
            Some(resolved.covered_through),
        )
        .await
        .unwrap();
        assert_eq!(kind(&outcome), "accepted");
        assert_eq!(dispatcher.calls()[2], vec![tickets[3], newest]);
    });
}
