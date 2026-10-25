//! End-to-end proof of issue #95's per-entity recurrence with
//! missed-run policy: a real `OrderPlaced` event schedules a recurring
//! deadline via the background poll task; each firing inserts the next
//! occurrence until the validity window ends, and a same-tagged
//! `OrderPaid` event cancelling the head ends the chain. Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness and one-test-
//! at-a-time discipline as `skilj/tests/deadlines.rs` - the background
//! deadline task scans every bounded context in the shared test database,
//! so this file's tests must never run alongside another file's ticks.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{
    CancelDeadline, CommandType, EventType, MissedRunPolicy, Projection, RecurrenceSpec,
    ScheduleDeadline, Skilj,
};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::{BoundedContextEvent, DeadlineSpec};
use skilj_core::shared::{
    generate_token_id, generate_token_secret, CommandDecision, EventSpec, Tag,
};
use tower::ServiceExt;

const ORDERS_BOUNDED_CONTEXT: &str = "skilj_deadline_recurrence_e2e_orders";

// --- source events ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OrderPlacedPayload {
    order_id: String,
    deadline_at: chrono::DateTime<chrono::Utc>,
}

struct OrderPlaced;

impl EventType for OrderPlaced {
    type Payload = OrderPlacedPayload;
    const NAME: &'static str = "OrderPlaced";
    const BOUNDED_CONTEXT: &'static str = ORDERS_BOUNDED_CONTEXT;
    fn direct_creation_allowed() -> bool {
        true
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OrderPaidPayload {
    order_id: String,
}

struct OrderPaid;

impl EventType for OrderPaid {
    type Payload = OrderPaidPayload;
    const NAME: &'static str = "OrderPaid";
    const BOUNDED_CONTEXT: &'static str = ORDERS_BOUNDED_CONTEXT;
    fn direct_creation_allowed() -> bool {
        true
    }
}

// --- fired commands + the events they produce ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct SendReminderPayload {
    order_id: String,
}

struct SendReminder;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct ReminderSentPayload {
    order_id: String,
}

struct ReminderSent;

impl EventType for ReminderSent {
    type Payload = ReminderSentPayload;
    const NAME: &'static str = "ReminderSent";
    const BOUNDED_CONTEXT: &'static str = ORDERS_BOUNDED_CONTEXT;
}

enum OrdersEvent {
    ReminderSent(ReminderSentPayload),
}

impl BoundedContextEvent for OrdersEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "ReminderSent" => {
                Some(serde_json::from_str(&event.payload).map(OrdersEvent::ReminderSent))
            }
            _ => None,
        }
    }
}

impl CommandType for SendReminder {
    type Payload = SendReminderPayload;
    type Event = OrdersEvent;
    const NAME: &'static str = "SendReminder";
    const BOUNDED_CONTEXT: &'static str = ORDERS_BOUNDED_CONTEXT;
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "ReminderSent".to_string(),
                payload: serde_json::json!({ "order_id": payload.order_id }),
            }],
        }
    }
}

// --- projections ---

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct RemindersSentState {
    reminded_order_ids: Vec<String>,
}

struct RemindersSent;

impl Projection for RemindersSent {
    type State = RemindersSentState;
    type Event = OrdersEvent;
    const NAME: &'static str = "RemindersSent";
    const BOUNDED_CONTEXT: &'static str = ORDERS_BOUNDED_CONTEXT;
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["ReminderSent"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        let OrdersEvent::ReminderSent(payload) = event;
        state.reminded_order_ids.push(payload.order_id.clone());
    }
}

// --- the recurring schedule ---

fn order_tag(order_id: &str) -> Tag {
    Tag {
        key: "order".to_string(),
        value: Some(order_id.to_string()),
    }
}

/// A reminder that recurs every second until one minute after its first
/// firing: second-granularity, so several occurrences land inside one
/// test's own patience, but `end_at` still stops the chain on its own -
/// the test proves both advancement *and* termination.
struct ScheduleRecurringReminder;

impl ScheduleDeadline for ScheduleRecurringReminder {
    type Source = OrderPlaced;
    type Target = SendReminder;
    const NAME: &'static str = "ScheduleRecurringReminder";
    fn schedule(source_payload: &OrderPlacedPayload) -> Option<DeadlineSpec<SendReminderPayload>> {
        Some(DeadlineSpec {
            fire_at: source_payload.deadline_at,
            expires_at: None,
            tags: vec![order_tag(&source_payload.order_id)],
            payload: SendReminderPayload {
                order_id: source_payload.order_id.clone(),
            },
            recurrence: Some(RecurrenceSpec {
                schedule: "* * * * * * *".to_string(),
                policy: MissedRunPolicy::FireAll,
                start_at: None,
                end_at: Some(source_payload.deadline_at + chrono::Duration::seconds(3)),
                valid_until: None,
            }),
        })
    }
}

struct CancelReminderOnPaid;

impl CancelDeadline for CancelReminderOnPaid {
    type Source = OrderPaid;
    type Deadline = ScheduleRecurringReminder;
    const NAME: &'static str = "CancelReminderOnPaid";
    fn cancel_tags(source_payload: &OrderPaidPayload) -> Option<Vec<Tag>> {
        Some(vec![order_tag(&source_payload.order_id)])
    }
}

// --- harness (mirrors deadlines.rs, not repeated) ---

struct TestDb {
    database_url: String,
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(|| async {
            let database_url =
                skilj_test_support::database_url("skilj_deadline_recurrence_e2e_test").await?;
            let pool = connect_and_migrate(&database_url, "the test database").await?;
            Some(TestDb { database_url, pool })
        })
        .await
        .as_ref()
        .map(|db| (db.database_url.clone(), db.pool.clone()))
}

async fn connect_and_migrate(database_url: &str, label: &str) -> Option<Pool> {
    let pool = match db::connect(database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to {label} failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating {label} failed: {e}");
        return None;
    }
    Some(pool)
}

static ONE_DEADLINE_TEST_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn create_direct_event(
    router: &axum::Router,
    credential: &str,
    payload: serde_json::Value,
) -> StatusCode {
    let body = serde_json::to_vec(&serde_json::json!({ "payload": payload })).unwrap();
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events/direct")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    response.status()
}

#[test]
fn a_recurring_deadline_fires_until_its_window_ends_and_a_cancel_stops_the_chain() {
    runtime().block_on(async {
        let _one_at_a_time = ONE_DEADLINE_TEST_AT_A_TIME.lock().await;
        let Some((database_url, pool)) = test_db().await else {
            return;
        };

        let external_subject = unique_name("subject");
        let role = Role {
            id: generate_token_id(),
            external_subject: external_subject.clone(),
            name: "Reconciliation Role".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &role).await.unwrap();

        let orders_bc = BoundedContext {
            name: ORDERS_BOUNDED_CONTEXT.to_string(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &orders_bc).await.unwrap();

        let orders_mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: orders_bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            capabilities: Default::default(),
            delegable: Default::default(),
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &orders_mapping)
            .await
            .unwrap();

        let (skilj, report) = Skilj::builder(database_url)
            .bounded_context(ORDERS_BOUNDED_CONTEXT)
            .event_type::<OrderPlaced>()
            .event_type::<OrderPaid>()
            .event_type::<ReminderSent>()
            .command_type::<SendReminder>()
            .projection::<RemindersSent>()
            .schedule_deadline::<ScheduleRecurringReminder>()
            .cancel_deadline::<CancelReminderOnPaid>()
            .deadline_poll_interval(std::time::Duration::from_millis(30))
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let order_placed_type = db::get_event_type(&pool, ORDERS_BOUNDED_CONTEXT, "OrderPlaced")
            .await
            .unwrap()
            .unwrap();
        let order_placed_token = access_control::create_direct_creation_token(
            &orders_mapping,
            &order_placed_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &order_placed_token)
            .await
            .unwrap();
        let order_placed_credential =
            format!("{}.{}", order_placed_token.id, order_placed_token.secret);

        let order_paid_type = db::get_event_type(&pool, ORDERS_BOUNDED_CONTEXT, "OrderPaid")
            .await
            .unwrap()
            .unwrap();
        let order_paid_token = access_control::create_direct_creation_token(
            &orders_mapping,
            &order_paid_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &order_paid_token)
            .await
            .unwrap();
        let order_paid_credential = format!("{}.{}", order_paid_token.id, order_paid_token.secret);

        let router = skilj.rest_router();

        // Two orders, first firing ~300ms out:
        //
        //   order-recurring: never paid - the chain fires its first
        //     occurrence, then one per second, until `end_at` (3s after the
        //     first) stops it on its own.
        //   order-cancelled: paid right away - the cancel ends the chain
        //     before, or shortly after, its first firing, so it must see
        //     at most one reminder, never the full chain.
        let deadline_at = test_now() + chrono::Duration::milliseconds(300);
        for order_id in ["order-recurring", "order-cancelled"] {
            let status = create_direct_event(
                &router,
                &order_placed_credential,
                serde_json::json!({ "order_id": order_id, "deadline_at": deadline_at }),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED);
        }
        let status = create_direct_event(
            &router,
            &order_paid_credential,
            serde_json::json!({ "order_id": "order-cancelled" }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        // Wait for the recurring chain to settle: `end_at` plus several
        // poll intervals of margin, so the "chain terminated on its own"
        // assertion below is meaningful rather than "didn't wait long
        // enough."
        let mut recurring = 0_usize;
        let mut cancelled = 0_usize;
        for _ in 0..200 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let state =
                db::get_projection_state(&pool, ORDERS_BOUNDED_CONTEXT, "RemindersSent", "")
                    .await
                    .unwrap();
            if let Some(state) = state {
                let parsed: RemindersSentState = serde_json::from_str(&state).unwrap();
                recurring = parsed
                    .reminded_order_ids
                    .iter()
                    .filter(|id| *id == "order-recurring")
                    .count();
                cancelled = parsed
                    .reminded_order_ids
                    .iter()
                    .filter(|id| *id == "order-cancelled")
                    .count();
                // First firing + ~3 one-second occurrences, then the
                // window ends the chain: settled once the recurring order
                // has at least 3 reminders and a full extra second passes
                // with no new ones.
                if recurring >= 3 {
                    let settled = recurring;
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    let state = db::get_projection_state(
                        &pool,
                        ORDERS_BOUNDED_CONTEXT,
                        "RemindersSent",
                        "",
                    )
                    .await
                    .unwrap();
                    if let Some(state) = state {
                        let parsed: RemindersSentState = serde_json::from_str(&state).unwrap();
                        let now_recurring = parsed
                            .reminded_order_ids
                            .iter()
                            .filter(|id| *id == "order-recurring")
                            .count();
                        if now_recurring == settled {
                            recurring = now_recurring;
                            break;
                        }
                        recurring = now_recurring;
                    }
                }
            }
        }

        // The chain fired repeatedly on its own, then stopped at the
        // window - not one-shot, not unbounded.
        assert!(
            (3..=6).contains(&recurring),
            "expected 3-6 reminders for the recurring order, saw {recurring}"
        );
        // The paid order's chain was cut short - at most its first firing
        // raced the cancel through, never the full chain.
        assert!(
            cancelled <= 1,
            "expected at most 1 reminder for the cancelled order, saw {cancelled}"
        );

        skilj.shutdown(std::time::Duration::from_secs(20)).await;
    });
}
