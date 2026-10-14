//! Tag-read benchmark: the reads that find a bounded context's events by
//! tag, on a large history - not a test, so `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --release -p skilj --test tag_read_bench -- --ignored --nocapture
//! ```
//!
//! (`DATABASE_URL` to run against a server of your own, otherwise the
//! embedded one; `SKILJ_BENCH_EVENTS` for another history size.) The
//! history is written with one `INSERT ... SELECT`, not through commands:
//! what is measured is reading it. Every event carries an `account` and a
//! `customer` tag and is one of three types. One account is long-lived,
//! with a tenth of the history; every other account has ten events.
//!
//! Each row is the median of several runs of one read, through the public
//! function a command or `queryEvents` reaches it by, with an empty event
//! cache so it goes to Postgres:
//!
//! - **tags**: `list_events_for_bounded_context_matching_tags` - the OR of
//!   the tags, what a command without a consistency query reads, and its
//!   re-check under the lock (the `after` rows);
//! - **items**: `list_events_matching_query_cached` - a consistency query
//!   item with and without event types, whole or latest only.
//!
//! The last rows time the blind-tag rewrites, which find their events by
//! tag too: erasure (median of five accounts) with the stored commands - one
//! per event - scanned, and again with a GIN index on their tags, then one
//! rotation. See docs/architecture.md §210 for the numbers.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{
    generate_token_id, CommandDecision, EventSpec, QueryItem, Tag, TagMapping,
};
use std::time::{Duration, Instant};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct AccountPayload {
    account_id: String,
    customer_id: String,
    amount: i64,
}

fn tag_mappings() -> Vec<TagMapping> {
    vec![
        TagMapping::new("account", "account_id"),
        TagMapping::new("customer", "customer_id"),
    ]
}

/// The command every stored command row is of. Its history isn't read.
struct Deposit;
enum NoHistory {}
impl BoundedContextEvent for NoHistory {
    fn try_from_event(_: &Event) -> Option<Result<Self, serde_json::Error>> {
        None
    }
}
impl CommandType for Deposit {
    type Payload = AccountPayload;
    type Event = NoHistory;
    const NAME: &'static str = "Deposit";
    fn tag_mappings() -> Vec<TagMapping> {
        tag_mappings()
    }
    fn decide(payload: &Self::Payload, _: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "Deposited".to_string(),
                payload: serde_json::to_value(payload).unwrap(),
            }],
        }
    }
}

struct Opened;
impl EventType for Opened {
    type Payload = AccountPayload;
    const NAME: &'static str = "Opened";
    fn tag_mappings() -> Vec<TagMapping> {
        tag_mappings()
    }
}

struct Deposited;
impl EventType for Deposited {
    type Payload = AccountPayload;
    const NAME: &'static str = "Deposited";
    fn tag_mappings() -> Vec<TagMapping> {
        tag_mappings()
    }
}

struct Withdrawn;
impl EventType for Withdrawn {
    type Payload = AccountPayload;
    const NAME: &'static str = "Withdrawn";
    fn tag_mappings() -> Vec<TagMapping> {
        tag_mappings()
    }
}

/// The long-lived account.
const HOT: &str = "acc-hot";

fn now() -> chrono::DateTime<chrono::Utc> {
    use chrono::SubsecRound;
    chrono::Utc::now().trunc_subsecs(6)
}

/// A bounded context with the three types registered, and its name.
async fn bounded_context(database_url: &str, pool: &Pool) -> (Skilj, String) {
    let subject = format!("subject_{}", generate_token_id());
    let role = Role {
        id: generate_token_id(),
        external_subject: subject.clone(),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();
    let name = format!("tags_{}", generate_token_id());
    let bc = BoundedContext {
        name: name.clone(),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    db::insert_role_access_mapping(
        pool,
        &RoleAccessMapping {
            role,
            bounded_context: bc,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: now(),
            revoked_at: None,
        },
    )
    .await
    .unwrap();
    let (skilj, _) = Skilj::builder(database_url)
        .bounded_context(name.clone())
        .event_type::<Opened>()
        .event_type::<Deposited>()
        .event_type::<Withdrawn>()
        .command_type::<Deposit>()
        .reconciliation_role(subject)
        .build()
        .await
        .unwrap();
    (skilj, name)
}

/// `events` events: every tenth of the hot account, the rest ten per
/// account; the types cycle; `customer` is `cust-` the account number
/// modulo 20,000. Sequences 1..=events. And a stored `Deposit` command per
/// event with the same tags, as a bounded context accumulates.
async fn write_history(pool: &Pool, bc: &str, events: i64) {
    let schema = format!("\"bc_{bc}\"");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "WITH g AS (SELECT s, \
            CASE WHEN s % 10 = 0 THEN '{HOT}' ELSE 'acc-' || (s / 10) END AS account, \
            'cust-' || ((s / 10) % 20000) AS customer FROM generate_series(1, $1) s) \
         INSERT INTO {schema}.events (sequence, event_type_name, payload, metadata_type, \
            metadata_version, metadata_client_id, metadata_created_at, tags, origin_kind) \
         SELECT s, (ARRAY['Opened', 'Deposited', 'Withdrawn'])[1 + s % 3], \
            json_build_object('account_id', account, 'customer_id', customer, 'amount', s)::text, \
            'event', 1, 'bench', now(), \
            jsonb_build_array(jsonb_build_object('key', 'account', 'value', account), \
                jsonb_build_object('key', 'customer', 'value', customer)), \
            'directly_created' \
         FROM g"
    )))
    .bind(events)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.commands (external_id, command_type_name, payload, \
            metadata_type, metadata_version, metadata_client_id, metadata_created_at, \
            consistency_tags) \
         SELECT 'cmd-' || e.sequence, 'Deposit', e.payload, 'command', 1, 'bench', now(), e.tags \
         FROM {schema}.events e"
    )))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.sequence SET next_value = $1"
    )))
    .bind(events)
    .execute(pool)
    .await
    .unwrap();
    for table in ["events", "event_tags", "commands"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "VACUUM ANALYZE {schema}.{table}"
        )))
        .execute(pool)
        .await
        .unwrap();
    }
}

fn tag(key: &str, value: &str) -> Tag {
    Tag {
        key: key.to_string(),
        value: Some(value.to_string()),
    }
}

fn item(types: &[&str], tags: Vec<Tag>, latest: bool) -> QueryItem {
    QueryItem {
        event_types: types.iter().map(|t| t.to_string()).collect(),
        tags,
        latest: latest.then_some(1),
        snapshot: None,
    }
}

/// The median of `runs` timings of `read`, and the events it returned.
async fn median<F, Fut>(runs: usize, mut read: F) -> (Duration, usize)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    let mut timings = Vec::with_capacity(runs);
    let mut found = 0;
    for _ in 0..runs {
        let started = Instant::now();
        found = read().await;
        timings.push(started.elapsed());
    }
    timings.sort();
    (timings[runs / 2], found)
}

fn row(name: &str, (took, found): (Duration, usize)) {
    println!(
        "{name:<44} {:>10.3} ms {found:>8} events",
        took.as_secs_f64() * 1000.0
    );
}

#[test]
#[ignore = "benchmark - run explicitly, see the module docs"]
fn tag_read_bench() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let database_url = skilj_test_support::database_url("skilj_tag_read_bench")
            .await
            .expect("a database: DATABASE_URL or the embedded one");
        let pool = db::connect(&database_url).await.unwrap();
        db::migrate(&pool).await.unwrap();
        let events: i64 = std::env::var("SKILJ_BENCH_EVENTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000);
        let (skilj, bc) = bounded_context(&database_url, &pool).await;
        let started = Instant::now();
        write_history(&pool, &bc, events).await;
        println!(
            "{events} events written in {:.1} s",
            started.elapsed().as_secs_f64()
        );
        let head = events;

        let tags = |tags: Vec<Tag>, after: Option<i64>| {
            let pool = pool.clone();
            let bc = bc.clone();
            move || {
                let pool = pool.clone();
                let bc = bc.clone();
                let tags = tags.clone();
                async move {
                    db::list_events_for_bounded_context_matching_tags(&pool, &bc, &tags, after)
                        .await
                        .unwrap()
                        .len()
                }
            }
        };
        let items = |query: Vec<QueryItem>, after: Option<i64>| {
            let pool = pool.clone();
            let bc = bc.clone();
            move || {
                let pool = pool.clone();
                let bc = bc.clone();
                let query = query.clone();
                async move {
                    db::list_events_matching_query_cached(
                        &pool,
                        &EventCache::new(0),
                        &bc,
                        &query,
                        after,
                    )
                    .await
                    .unwrap()
                    .0
                    .len()
                }
            }
        };
        let short = format!("acc-{}", events / 20);
        let all = ["Opened", "Deposited", "Withdrawn"];

        println!("{:<44} {:>13} {:>15}", "read", "median", "found");
        row(
            "tags: short account",
            median(21, tags(vec![tag("account", &short)], None)).await,
        );
        row(
            "tags: short account + 2 customers",
            median(
                21,
                tags(
                    vec![
                        tag("account", &short),
                        tag("customer", "cust-7"),
                        tag("customer", "cust-8"),
                    ],
                    None,
                ),
            )
            .await,
        );
        row(
            "tags: hot account, whole history",
            median(5, tags(vec![tag("account", HOT)], None)).await,
        );
        row(
            "tags: hot account, after head - 100",
            median(21, tags(vec![tag("account", HOT)], Some(head - 100))).await,
        );
        row(
            "tags: short account, after head - 100",
            median(21, tags(vec![tag("account", &short)], Some(head - 100))).await,
        );
        row(
            "items: short account, no types",
            median(21, items(vec![item(&[], vec![tag("account", &short)], false)], None)).await,
        );
        row(
            "items: hot account, no types, latest",
            median(21, items(vec![item(&[], vec![tag("account", HOT)], true)], None)).await,
        );
        row(
            "items: hot account, all types, latest",
            median(21, items(vec![item(&all, vec![tag("account", HOT)], true)], None)).await,
        );
        row(
            "items: hot account, no types, after head - 100",
            median(
                21,
                items(vec![item(&[], vec![tag("account", HOT)], false)], Some(head - 100)),
            )
            .await,
        );

        // The blind-tag rewrites, once each: a blind `customer` constraint
        // erased with its account.
        let schema = format!("\"bc_{bc}\"");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.blind_tags (tag_key, normalizer_version, key_generation, \
             erasure, registered_at) VALUES ('customer', 1, 0, \
             '{{\"kind\": \"with_subject\", \"subject_key\": \"account\", \"subject_tag\": \"account\"}}', \
             now())"
        )))
        .execute(&pool)
        .await
        .unwrap();
        // Five accounts each, a different five per row.
        let erase = |first: i64| {
            let pool = pool.clone();
            let bc = bc.clone();
            let mut next = first;
            move || {
                let pool = pool.clone();
                let bc = bc.clone();
                let account = format!("acc-{next}");
                next += 1;
                async move {
                    db::forget_subject_in_blind_tags(&pool, &bc, "account", &account)
                        .await
                        .unwrap() as usize
                }
            }
        };
        row(
            "erasure: one account's blind tag",
            median(5, erase(events / 20)).await,
        );
        // Whether an index on the commands' tags would pay: the same, with one.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE INDEX commands_by_tags ON {schema}.commands \
             USING GIN (consistency_tags jsonb_path_ops)"
        )))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!("ANALYZE {schema}.commands")))
            .execute(&pool)
            .await
            .unwrap();
        row(
            "erasure: the same, commands GIN-indexed",
            median(5, erase(events / 20 + 100)).await,
        );
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP INDEX {schema}.commands_by_tags"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let started = Instant::now();
        let wrap = |t: &str| -> skilj_core::error::Result<String> { Ok(format!("{t}'")) };
        assert!(db::rotate_blind_tag(&pool, &bc, "customer", 0, &wrap, "fingerprint")
            .await
            .unwrap());
        row(
            "rotation: every customer tag",
            (started.elapsed(), events as usize),
        );

        skilj.shutdown(Duration::from_secs(10)).await;
    });
}
