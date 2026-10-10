//! What a command's read costs as its company's history grows, with and
//! without a consistency query (docs/architecture.md §198) - not a test,
//! so `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --release -p skilj-core --test consistency_query_read -- --ignored --nocapture
//! ```
//!
//! (`DATABASE_URL` to run against a server of your own, otherwise the
//! embedded one.) For each history size, one company signs up and files
//! that many tickets. Then the read before the lock
//! (`db::resolve_command_submission`, event cache off) of a new ticket is
//! timed for two command types:
//!
//! - **tags**: tagged by company, no query - every event of the company,
//!   as the helpdesk's `CreateTicket` read before its snapshot;
//! - **query**: the company's latest lifecycle event, and `TicketCreated`
//!   for its own ticket id - two index lookups.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, CommandType, Event, EventOrigin, EventType,
};
use skilj_core::plugin::{CommandDispatcher, SnapshotDispatcher};
use skilj_core::shared::{
    generate_token_id, CommandDecision, Metadata, QueryItemMapping, TagMapping,
};
use std::time::{Duration, Instant};

const READS: usize = 50;

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

struct Dispatcher;

impl CommandDispatcher for Dispatcher {
    fn dispatch(
        &self,
        _: &str,
        _: &str,
        _: &str,
        matching_events: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        Some(Ok(CommandDecision::Rejected {
            reason: matching_events.len().to_string(),
            kind: "bench".to_string(),
        }))
    }

    fn required_role(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        Some(None)
    }

    fn snapshot_name(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        Some(None)
    }

    fn consistency_query(&self, _: &str, command_type: &str) -> Option<Vec<QueryItemMapping>> {
        (command_type == "CreateTicketByQuery").then(|| {
            vec![
                QueryItemMapping::types(&["CompanySignedUp"])
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

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio runtime"))
}

async fn pool() -> Option<Pool> {
    let url = skilj_test_support::database_url("skilj_consistency_query_read").await?;
    let pool = db::connect(&url).await.ok()?;
    db::migrate(&pool).await.ok()?;
    Some(pool)
}

fn now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

fn event_type(bc: &BoundedContext, name: &str, tag_mappings: Vec<TagMapping>) -> EventType {
    EventType {
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
    }
}

fn command_type(bc: &BoundedContext, name: &str) -> CommandType {
    CommandType {
        bounded_context: bc.clone(),
        name: name.to_string(),
        schema: r#"{"properties":{"company_id":{"type":"string"},"ticket_id":{"type":"string"}}}"#
            .to_string(),
        schema_version: 1,
        tag_mappings: vec![company(), ticket()],
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        rest_trigger_allowed: true,
    }
}

/// A bounded context where one company signed up and filed `tickets`
/// tickets.
async fn seed(pool: &Pool, tickets: usize) -> (CommandType, CommandType) {
    let bc = BoundedContext {
        name: format!("bench_{}", generate_token_id()),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    let signed_up = event_type(&bc, "CompanySignedUp", vec![company()]);
    let created = event_type(&bc, "TicketCreated", vec![company(), ticket()]);
    db::upsert_event_type(pool, &signed_up).await.unwrap();
    db::upsert_event_type(pool, &created).await.unwrap();
    // The tag-only command tags by company alone, as `CreateTicket` did.
    let by_tags = CommandType {
        tag_mappings: vec![company()],
        ..command_type(&bc, "CreateTicketByTags")
    };
    let by_query = command_type(&bc, "CreateTicketByQuery");
    db::upsert_command_type(pool, &by_tags).await.unwrap();
    db::upsert_command_type(pool, &by_query).await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    for i in 0..=tickets {
        let (et, payload) = if i == 0 {
            (&signed_up, r#"{"company_id":"acme"}"#.to_string())
        } else {
            (
                &created,
                format!(r#"{{"company_id":"acme","ticket_id":"t{i}"}}"#),
            )
        };
        let sequence = db::next_sequence(&mut *tx, &bc.name).await.unwrap();
        let event = Event {
            bounded_context: bc.clone(),
            event_type: et.clone(),
            tags: skilj_core::event_store::derive_tags(&et.tag_mappings, &payload),
            payload,
            metadata: Metadata {
                r#type: et.name.clone(),
                version: 1,
                client_id: "bench".to_string(),
                created_at: now(),
                correlation_id: None,
                causation_id: None,
            },
            sequence,
            encryption_keys: Vec::new(),
            origin: EventOrigin::DirectlyCreated,
        };
        db::insert_event(&mut *tx, &event, None).await.unwrap();
    }
    tx.commit().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ANALYZE \"bc_{}\".events",
        bc.name
    )))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ANALYZE \"bc_{}\".event_tags",
        bc.name
    )))
    .execute(pool)
    .await
    .unwrap();
    (by_tags, by_query)
}

/// The median read, and how many events it handed `decide()`.
async fn time_reads(pool: &Pool, command_type: &CommandType) -> (Duration, usize) {
    let cache = EventCache::new(0);
    let payload = r#"{"company_id":"acme","ticket_id":"new"}"#;
    let mut times = Vec::with_capacity(READS);
    let mut seen = 0;
    for _ in 0..READS {
        let started = Instant::now();
        let resolved = db::resolve_command_submission(
            pool,
            &Dispatcher,
            &NoSnapshots,
            &cache,
            command_type,
            payload,
        )
        .await
        .unwrap();
        times.push(started.elapsed());
        seen = resolved.matching_events.len();
    }
    times.sort();
    (times[READS / 2], seen)
}

#[test]
#[ignore = "benchmark - run explicitly, see the module doc comment"]
fn consistency_query_read() {
    runtime().block_on(async {
        let Some(pool) = pool().await else {
            eprintln!("skipping: no Postgres available");
            return;
        };
        for tickets in [100, 1_000, 10_000, 50_000] {
            let (by_tags, by_query) = seed(&pool, tickets).await;
            for (label, command_type) in [("tags", &by_tags), ("query", &by_query)] {
                let (median, seen) = time_reads(&pool, command_type).await;
                println!(
                    "tickets={tickets:<6} {label:<6} median={:>8.2} ms  events to decide()={seen}",
                    median.as_secs_f64() * 1000.0
                );
            }
        }
    });
}
