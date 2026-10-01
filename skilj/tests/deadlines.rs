//! End-to-end proof of Codeberg issue #20's native one-shot, per-entity
//! deadline mechanism (`skilj_core::plugin::ScheduleDeadline`/
//! `CancelDeadline`): a real `OrderPlaced` event, directly created,
//! schedules a deadline via the background poll task
//! `SkiljBuilder::build()` spawns for it; that deadline either fires a
//! real `CancelOrder` command once due, or never does when a same-tagged
//! `OrderPaid` event cancels it first - proof the wiring from
//! `skilj_core::db::catch_up_schedule_deadline`/`catch_up_cancel_deadline`/
//! `fire_due_deadlines` all the way through `SkiljBuilder::schedule_deadline::<S>()`/
//! `.cancel_deadline::<C>()` is real and running, not just the
//! persistence layer in isolation. Same `DATABASE_URL`-then-embedded-
//! Postgres-then-skip harness as `skilj/tests/cross_context_route.rs` -
//! see its own doc comment for the details, not repeated a third time
//! here.
//!
//! One bounded context, one test function, three orders as sub-scenarios,
//! the same "several sub-scenarios in one shared harness" shape
//! `cross_context_route.rs`'s own main test already uses. Chosen here to
//! keep this file's own share of the pre-existing embedded-Postgres
//! connection-pool pressure (see `CONTRIBUTING.md`) as small as
//! reasonably possible: every additional `#[test]` fn builds its own
//! `Skilj` instance, and this feature alone adds three more perpetual
//! background poll tasks per instance on top of the five every other
//! test file's instance already carries.
//!
//! **Not covered here**: a dedicated crash/redelivery simulation proving
//! `ON CONFLICT (id) DO NOTHING` makes a redelivered schedule-catch-up
//! tick a safe no-op. That property is structural, not incidental - the
//! identical mechanism (a deterministic id/idempotency key,
//! `Deduplicated` handled as a warning not an error) `CrossContextRoute`
//! already relies on and which docs/architecture.md §36/§37 reasoned
//! about at length - and `cross_context_route.rs`'s own three tests don't
//! carry a dedicated crash-simulation test for it either, for the same
//! reason this file doesn't: the black-box, full-`Skilj` harness every
//! test file here uses has no fault-injection hook to force a real
//! redelivery, only ever exercising the ordinary, non-redelivered path.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CancelDeadline, CommandType, EventType, Projection, ScheduleDeadline, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::{BoundedContextEvent, DeadlineSpec};
use skilj_core::shared::{
    generate_token_id, generate_token_secret, CommandDecision, EventSpec, Tag,
};
use tower::ServiceExt;

const ORDERS_BOUNDED_CONTEXT: &str = "skilj_deadlines_test_orders";

// --- source events ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OrderPlacedPayload {
    order_id: String,
    deadline_at: chrono::DateTime<chrono::Utc>,
    /// Exercises `ScheduleOrderCancelDeadline::schedule` returning
    /// `None` (the occurrence this schedule itself decided doesn't
    /// apply) when `false` - `ScheduleReminderDeadline` below ignores
    /// this field entirely and always schedules its own reminder
    /// regardless, proving one schedule's own skip has no bearing on a
    /// sibling schedule reacting to the identical event.
    schedule_cancel_deadline: bool,
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
struct CancelOrderPayload {
    order_id: String,
}

struct CancelOrder;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OrderCancelledPayload {
    order_id: String,
}

struct OrderCancelled;

impl EventType for OrderCancelled {
    type Payload = OrderCancelledPayload;
    const NAME: &'static str = "OrderCancelled";
    const BOUNDED_CONTEXT: &'static str = ORDERS_BOUNDED_CONTEXT;
}

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
    OrderCancelled(OrderCancelledPayload),
    ReminderSent(ReminderSentPayload),
}

impl BoundedContextEvent for OrdersEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "OrderCancelled" => {
                Some(serde_json::from_str(&event.payload).map(OrdersEvent::OrderCancelled))
            }
            "ReminderSent" => {
                Some(serde_json::from_str(&event.payload).map(OrdersEvent::ReminderSent))
            }
            _ => None,
        }
    }
}

impl CommandType for CancelOrder {
    type Payload = CancelOrderPayload;
    type Event = OrdersEvent;
    const NAME: &'static str = "CancelOrder";
    const BOUNDED_CONTEXT: &'static str = ORDERS_BOUNDED_CONTEXT;
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "OrderCancelled".to_string(),
                payload: serde_json::json!({ "order_id": payload.order_id }),
            }],
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
struct OrderCancellationsState {
    cancelled_order_ids: Vec<String>,
}

/// `sync: true` - the assertions below are already only waiting on the
/// deadline background tasks' own poll cycles; no reason to also make
/// them wait on a second, independent projection poll task on top (same
/// reasoning `cross_context_route.rs`'s own `ReservedTotal` gives).
struct OrderCancellations;

impl Projection for OrderCancellations {
    type State = OrderCancellationsState;
    type Event = OrdersEvent;
    const NAME: &'static str = "OrderCancellations";
    const BOUNDED_CONTEXT: &'static str = ORDERS_BOUNDED_CONTEXT;
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["OrderCancelled"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        if let OrdersEvent::OrderCancelled(payload) = event {
            state.cancelled_order_ids.push(payload.order_id.clone());
        }
    }
}

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
        if let OrdersEvent::ReminderSent(payload) = event {
            state.reminded_order_ids.push(payload.order_id.clone());
        }
    }
}

// --- the deadline reactors themselves ---

fn order_tag(order_id: &str) -> Tag {
    Tag {
        key: "order".to_string(),
        value: Some(order_id.to_string()),
    }
}

struct ScheduleOrderCancelDeadline;

impl ScheduleDeadline for ScheduleOrderCancelDeadline {
    type Source = OrderPlaced;
    type Target = CancelOrder;
    const NAME: &'static str = "ScheduleOrderCancelDeadline";
    fn schedule(source_payload: &OrderPlacedPayload) -> Option<DeadlineSpec<CancelOrderPayload>> {
        if !source_payload.schedule_cancel_deadline {
            return None;
        }
        Some(DeadlineSpec {
            fire_at: source_payload.deadline_at,
            tags: vec![order_tag(&source_payload.order_id)],
            payload: CancelOrderPayload {
                order_id: source_payload.order_id.clone(),
            },
        })
    }
}

/// Shares `order_tag(...)` with `ScheduleOrderCancelDeadline` above -
/// deliberately, to prove `CancelDeadline::Deadline`'s own
/// `schedule_name` scoping: cancelling one schedule's rows for a given
/// tag must never touch a different schedule's own rows for that
/// identical tag.
struct ScheduleReminderDeadline;

impl ScheduleDeadline for ScheduleReminderDeadline {
    type Source = OrderPlaced;
    type Target = SendReminder;
    const NAME: &'static str = "ScheduleReminderDeadline";
    fn schedule(source_payload: &OrderPlacedPayload) -> Option<DeadlineSpec<SendReminderPayload>> {
        Some(DeadlineSpec {
            fire_at: source_payload.deadline_at,
            tags: vec![order_tag(&source_payload.order_id)],
            payload: SendReminderPayload {
                order_id: source_payload.order_id.clone(),
            },
        })
    }
}

struct CancelOrderDeadlineOnPaid;

impl CancelDeadline for CancelOrderDeadlineOnPaid {
    type Source = OrderPaid;
    type Deadline = ScheduleOrderCancelDeadline;
    const NAME: &'static str = "CancelOrderDeadlineOnPaid";
    fn cancel_tags(source_payload: &OrderPaidPayload) -> Option<Vec<Tag>> {
        Some(vec![order_tag(&source_payload.order_id)])
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---
// (identical shape to `cross_context_route.rs`'s own harness - see that
// file's doc comment)

struct TestDb {
    database_url: String,
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for deadline tests")
    })
}

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(provision)
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

async fn provision() -> Option<TestDb> {
    let database_url = skilj_test_support::database_url("skilj_deadlines_e2e_test").await?;

    let pool = connect_and_migrate(&database_url, "the test database").await?;
    Some(TestDb { database_url, pool })
}

/// Held by every test in this file for its whole run. Each `Skilj` built
/// here runs a background deadline tick - at least the one it runs straight
/// away - and that tick scans *every* bounded context in the shared test
/// database, so while one test's `Skilj` is ticking, another test's due
/// deadline can be claimed out from under it (seen as a deadline left
/// `firing` instead of `fired`). Run one at a time, each stopping its
/// `Skilj` before it lets go, no test sees another's ticks
/// (docs/architecture.md §146).
static ONE_DEADLINE_TEST_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A built `Skilj`'s dispatchers, with its background tasks stopped - for
/// a test that fires deadlines with its own ticks. An hour-long interval
/// isn't enough on its own: the background deadline task ticks once
/// straight away, on a spawned task that can run late, after the test has
/// inserted a due deadline (docs/architecture.md §146). `shutdown` waits
/// for that tick to finish.
async fn dispatchers_without_background_ticks(
    skilj: Skilj,
) -> (
    std::sync::Arc<dyn skilj_core::plugin::CommandDispatcher>,
    std::sync::Arc<dyn skilj_core::plugin::ProjectionDispatcher>,
    std::sync::Arc<dyn skilj_core::plugin::SnapshotDispatcher>,
) {
    let dispatchers = (
        skilj.command_dispatcher(),
        skilj.projection_dispatcher(),
        skilj.snapshot_dispatcher(),
    );
    skilj.shutdown(std::time::Duration::from_secs(20)).await;
    dispatchers
}

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
fn a_deadline_fires_when_due_and_never_fires_once_cancelled_by_tag() {
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
            .event_type::<OrderCancelled>()
            .event_type::<ReminderSent>()
            .command_type::<CancelOrder>()
            .command_type::<SendReminder>()
            .projection::<OrderCancellations>()
            .projection::<RemindersSent>()
            .schedule_deadline::<ScheduleOrderCancelDeadline>()
            .cancel_deadline::<CancelOrderDeadlineOnPaid>()
            .schedule_deadline::<ScheduleReminderDeadline>()
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

        // Three orders, one shared deadline window (`deadline_at`, 300ms
        // out - several multiples of the 30ms poll interval above, so
        // every background task gets several ticks' worth of margin on
        // both sides of it):
        //
        //   order-A: paid immediately, before its own deadline - the
        //     `CancelOrder` deadline must be cancelled and must never
        //     fire, but the *reminder* deadline (a different schedule,
        //     sharing the identical `order` tag) must still fire -
        //     `CancelDeadline::Deadline`'s own scoping at work.
        //   order-B: never paid - its `CancelOrder` deadline fires
        //     normally, same as its reminder.
        //   order-C: `schedule_cancel_deadline: false` - `ScheduleOrderCancelDeadline::schedule`
        //     itself decides this occurrence doesn't apply, so no
        //     `CancelOrder` deadline is ever scheduled at all; its
        //     reminder still fires, unaffected by the sibling schedule's
        //     own skip.
        let deadline_at = test_now() + chrono::Duration::milliseconds(300);

        for (order_id, schedule_cancel_deadline) in
            [("order-A", true), ("order-B", true), ("order-C", false)]
        {
            let status = create_direct_event(
                &router,
                &order_placed_credential,
                serde_json::json!({
                    "order_id": order_id,
                    "deadline_at": deadline_at,
                    "schedule_cancel_deadline": schedule_cancel_deadline,
                }),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED);
        }

        let status = create_direct_event(
            &router,
            &order_paid_credential,
            serde_json::json!({ "order_id": "order-A" }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        // Wait for every reminder to have fired - the longest-running
        // path here (it never gets cancelled for any of the three
        // orders), so once it's settled, `deadline_at` plus many poll
        // intervals' worth of margin has also long since passed for the
        // `CancelOrder` deadlines, making the negative assertions below
        // (order-A and order-C must never appear as cancelled)
        // meaningful rather than merely "didn't wait long enough."
        let mut reminders: Vec<String> = Vec::new();
        for _ in 0..80 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            let state =
                db::get_projection_state(&pool, ORDERS_BOUNDED_CONTEXT, "RemindersSent", "")
                    .await
                    .unwrap();
            if let Some(state) = state {
                let parsed: RemindersSentState = serde_json::from_str(&state).unwrap();
                reminders = parsed.reminded_order_ids;
                if reminders.len() >= 3 {
                    break;
                }
            }
        }
        reminders.sort();
        assert_eq!(
            reminders,
            vec![
                "order-A".to_string(),
                "order-B".to_string(),
                "order-C".to_string()
            ],
            "every order's own reminder deadline must fire, regardless of what happened to its \
             (differently-scheduled) CancelOrder deadline"
        );

        // Polled rather than read once: the reminders loop above only waits
        // on `RemindersSent`, and under a loaded machine the
        // `OrderCancellations` projection's own catch-up tick can land a
        // moment later than that one's.
        let mut cancellations_state = None;
        for _ in 0..80 {
            cancellations_state =
                db::get_projection_state(&pool, ORDERS_BOUNDED_CONTEXT, "OrderCancellations", "")
                    .await
                    .unwrap();
            if cancellations_state
                .as_deref()
                .and_then(|s| serde_json::from_str::<OrderCancellationsState>(s).ok())
                .is_some_and(|c| !c.cancelled_order_ids.is_empty())
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let cancellations_state = cancellations_state
            .expect("OrderCancellations projection state never appeared within 2s");
        let cancellations: OrderCancellationsState =
            serde_json::from_str(&cancellations_state).unwrap();
        assert_eq!(
            cancellations.cancelled_order_ids,
            vec!["order-B".to_string()],
            "order-A must have been cancelled by its own OrderPaid event before it came due, \
             and order-C must never have been scheduled at all - only order-B's CancelOrder \
             deadline should ever have fired"
        );
        // docs/architecture.md §162: a resolved deadline keeps no payload -
        // it was the target command's plaintext.
        let resolved: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT status, payload FROM {}.deadlines WHERE status NOT IN ('pending', 'firing')",
            race_schema(ORDERS_BOUNDED_CONTEXT)
        )))
        .fetch_all(&pool)
        .await
        .unwrap();
        for status in ["fired", "cancelled"] {
            assert!(
                resolved.iter().any(|(s, _)| s == status),
                "no {status} deadline: {resolved:?}"
            );
        }
        for (status, payload) in &resolved {
            assert_eq!(payload, "{}", "a {status} deadline kept its payload");
        }
        // Stopped before the lock is released - see `ONE_DEADLINE_TEST_AT_A_TIME`.
        skilj.shutdown(std::time::Duration::from_secs(20)).await;
    });
}

// --- fire-vs-cancel race regression (Codeberg issue #25 review,
// docs/architecture.md §55) ---
//
// Deliberately doesn't go through `ScheduleDeadline`/`CancelDeadline` at
// all - the property under test is `db::fire_due_deadlines`'s own
// claim-then-submit ordering against a concurrent `status = 'pending'`-
// guarded cancel, which is exactly `catch_up_cancel_deadline`'s own
// `UPDATE`'s shape regardless of which schedule/tag logic decided to run
// it. A hand-inserted `deadlines` row and a hand-issued cancel `UPDATE`
// exercise that shape directly and deterministically, rather than
// relying on `catch_up_cancel_deadline`'s own background poll tick to
// land inside a race window by chance.

const DEADLINE_RACE_BOUNDED_CONTEXT: &str = "skilj_deadlines_test_race";

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct RaceCommandPayload {
    order_id: String,
}

struct RaceCommand;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct RaceFiredPayload {
    order_id: String,
}

struct RaceFired;

impl EventType for RaceFired {
    type Payload = RaceFiredPayload;
    const NAME: &'static str = "RaceFired";
    const BOUNDED_CONTEXT: &'static str = DEADLINE_RACE_BOUNDED_CONTEXT;
}

enum RaceEvent {
    RaceFired(RaceFiredPayload),
}

impl BoundedContextEvent for RaceEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "RaceFired" => Some(serde_json::from_str(&event.payload).map(RaceEvent::RaceFired)),
            _ => None,
        }
    }
}

impl CommandType for RaceCommand {
    type Payload = RaceCommandPayload;
    type Event = RaceEvent;
    const NAME: &'static str = "RaceCommand";
    const BOUNDED_CONTEXT: &'static str = DEADLINE_RACE_BOUNDED_CONTEXT;
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "RaceFired".to_string(),
                payload: serde_json::json!({ "order_id": payload.order_id }),
            }],
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct RaceFiredOrdersState {
    order_ids: Vec<String>,
}

/// `sync: true` - the test below reads this state immediately after
/// `fire_due_deadlines` returns, with no poll loop of its own to wait
/// out.
struct RaceFiredOrders;

impl Projection for RaceFiredOrders {
    type State = RaceFiredOrdersState;
    type Event = RaceEvent;
    const NAME: &'static str = "RaceFiredOrders";
    const BOUNDED_CONTEXT: &'static str = DEADLINE_RACE_BOUNDED_CONTEXT;
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["RaceFired"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        let RaceEvent::RaceFired(payload) = event;
        state.order_ids.push(payload.order_id.clone());
    }
}

fn race_schema(bounded_context: &str) -> String {
    format!("\"bc_{bounded_context}\"")
}

#[test]
fn fire_due_deadlines_never_lets_a_claimed_row_also_get_cancelled() {
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

        let race_bc = BoundedContext {
            name: DEADLINE_RACE_BOUNDED_CONTEXT.to_string(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &race_bc).await.unwrap();

        let race_mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: race_bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &race_mapping)
            .await
            .unwrap();

        let (skilj, report) = Skilj::builder(database_url)
            // Only this test's own ticks fire deadlines: the background one
            // scans every bounded context and would race them - stopped
            // below (`dispatchers_without_background_ticks`).
            .deadline_poll_interval(std::time::Duration::from_secs(3600))
            .bounded_context(DEADLINE_RACE_BOUNDED_CONTEXT)
            .event_type::<RaceFired>()
            .command_type::<RaceCommand>()
            .projection::<RaceFiredOrders>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());
        let (command_dispatcher, projection_dispatcher, snapshot_dispatcher) =
            dispatchers_without_background_ticks(skilj).await;

        // A real due-and-`pending` row, inserted directly - see this
        // section's own doc comment for why `catch_up_schedule_deadline`
        // isn't used to create it here.
        let schema = race_schema(DEADLINE_RACE_BOUNDED_CONTEXT);
        let deadline_id = unique_name("race-deadline");
        let now = test_now();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.deadlines \
             (id, schedule_name, fire_at, tags, correlation_id, target_bounded_context, \
              target_command_type, payload, status, created_at) \
             VALUES ($1, 'race-schedule', $2, '[]'::jsonb, NULL, $3, $4, $5, 'pending', $6)"
        )))
        .bind(&deadline_id)
        .bind(now - chrono::Duration::seconds(1))
        .bind(DEADLINE_RACE_BOUNDED_CONTEXT)
        .bind(RaceCommand::NAME)
        .bind(serde_json::json!({ "order_id": "race-order" }).to_string())
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        let broadcaster = skilj_core::event_store::EventBroadcaster::new(16);
        let event_cache = skilj_core::event_cache::EventCache::new(0);

        // The real `fire_due_deadlines` racing against a hand-issued
        // `UPDATE` shaped exactly like `catch_up_cancel_deadline`'s own
        // cancel - concurrent, via `tokio::join!`, not sequenced with a
        // sleep. Whichever the database's own row-level locking lets win,
        // the assertion below holds either way (see this section's own
        // doc comment) - this isn't a race the test is trying to land
        // inside a narrow window, it's an invariant that must survive
        // either ordering.
        let retry_policy = skilj_retry::RetryPolicy::default();
        let fire = db::fire_due_deadlines(
            &pool,
            &*command_dispatcher,
            &*projection_dispatcher,
            &*snapshot_dispatcher,
            &broadcaster,
            &event_cache,
            DEADLINE_RACE_BOUNDED_CONTEXT,
            now,
            None,
            &retry_policy,
            &[],
        );
        let cancel = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.deadlines SET status = 'cancelled', resolved_at = $1 \
             WHERE id = $2 AND status = 'pending'"
        )))
        .bind(now)
        .bind(&deadline_id)
        .execute(&pool);

        let (fire_result, cancel_result) = tokio::join!(fire, cancel);
        fire_result.unwrap();
        cancel_result.unwrap();

        let (status,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT status FROM {schema}.deadlines WHERE id = $1"
        )))
        .bind(&deadline_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let fired_state =
            db::get_projection_state(&pool, DEADLINE_RACE_BOUNDED_CONTEXT, "RaceFiredOrders", "")
                .await
                .unwrap();
        let command_ran = fired_state
            .map(|s| {
                let parsed: RaceFiredOrdersState = serde_json::from_str(&s).unwrap();
                !parsed.order_ids.is_empty()
            })
            .unwrap_or(false);

        match status.as_str() {
            "cancelled" => assert!(
                !command_ran,
                "the row ended cancelled but its target command still ran - the \
                 fire-vs-cancel race is not closed"
            ),
            "fired" => assert!(
                command_ran,
                "the row ended fired but its target command never actually ran"
            ),
            other => panic!(
                "deadline ended in unexpected status {other:?} - claim/resolve did not \
                 complete on either side of the race"
            ),
        }
    });
}

/// docs/architecture.md §96: a deadline lives in its source bounded
/// context, whose fire tick keeps running - but its target is archived.
/// Archiving stops new commands, so the deadline must be resolved without
/// submitting, not left to be reclaimed and refused every few minutes
/// forever (and, before `process_command` checked, not submitted into the
/// archived context either).
#[test]
fn a_deadline_whose_target_is_archived_resolves_without_submitting() {
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
        let mut contexts = Vec::new();
        for prefix in ["source", "target"] {
            let bc = BoundedContext {
                name: unique_name(prefix),
                status: BoundedContextStatus::Active,
                created_at: test_now(),
                created_by: ContextCreator::SystemCreator,
                template: None,
            };
            db::insert_bounded_context(&pool, &bc).await.unwrap();
            db::insert_role_access_mapping(
                &pool,
                &RoleAccessMapping {
                    role: role.clone(),
                    bounded_context: bc.clone(),
                    level: AccessLevel::Admin,
                    can_read_sensitive: false,
                    scope: None,
                    status: RoleStatus::Active,
                    created_at: test_now(),
                    revoked_at: None,
                },
            )
            .await
            .unwrap();
            contexts.push(bc.name);
        }
        let (source, target) = (contexts[0].clone(), contexts[1].clone());

        let (skilj, report) = Skilj::builder(database_url)
            // Only this test's own ticks fire deadlines: the background one
            // scans every bounded context and would race them - stopped
            // below (`dispatchers_without_background_ticks`).
            .deadline_poll_interval(std::time::Duration::from_secs(3600))
            .bounded_context(target.clone())
            .event_type::<RaceFired>()
            .command_type::<RaceCommand>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());
        let (command_dispatcher, projection_dispatcher, snapshot_dispatcher) =
            dispatchers_without_background_ticks(skilj).await;
        db::update_bounded_context_status(&pool, &target, BoundedContextStatus::Archived)
            .await
            .unwrap();

        let deadline_id = unique_name("deadline");
        let now = test_now();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {}.deadlines \
             (id, schedule_name, fire_at, tags, correlation_id, target_bounded_context, \
              target_command_type, payload, status, created_at) \
             VALUES ($1, 'schedule', $2, '[]'::jsonb, NULL, $3, $4, $5, 'pending', $6)",
            race_schema(&source)
        )))
        .bind(&deadline_id)
        .bind(now - chrono::Duration::seconds(1))
        .bind(&target)
        .bind(RaceCommand::NAME)
        .bind(serde_json::json!({ "order_id": "order-1" }).to_string())
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        db::fire_due_deadlines(
            &pool,
            &*command_dispatcher,
            &*projection_dispatcher,
            &*snapshot_dispatcher,
            &skilj_core::event_store::EventBroadcaster::new(16),
            &skilj_core::event_cache::EventCache::new(0),
            &source,
            now,
            None,
            &skilj_retry::RetryPolicy::default(),
            &[],
        )
        .await
        .unwrap();

        let (status,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT status FROM {}.deadlines WHERE id = $1",
            race_schema(&source)
        )))
        .bind(&deadline_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status, "fired");
        assert!(db::list_events_for_bounded_context(&pool, &target)
            .await
            .unwrap()
            .is_empty());
    });
}

/// docs/architecture.md §115: a deadline whose command fails with an
/// error (here a payload its command can't deserialize) is retried with
/// backoff and then parked - not re-fired every few minutes forever
/// behind a stale `firing` claim, and not allowed to stop the rest of
/// the tick. A valid deadline due right after it still fires.
#[test]
fn a_failing_deadline_is_retried_then_parked_without_blocking_others() {
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
        let mut contexts = Vec::new();
        for prefix in ["source", "target"] {
            let bc = BoundedContext {
                name: unique_name(prefix),
                status: BoundedContextStatus::Active,
                created_at: test_now(),
                created_by: ContextCreator::SystemCreator,
                template: None,
            };
            db::insert_bounded_context(&pool, &bc).await.unwrap();
            db::insert_role_access_mapping(
                &pool,
                &RoleAccessMapping {
                    role: role.clone(),
                    bounded_context: bc.clone(),
                    level: AccessLevel::Admin,
                    can_read_sensitive: false,
                    scope: None,
                    status: RoleStatus::Active,
                    created_at: test_now(),
                    revoked_at: None,
                },
            )
            .await
            .unwrap();
            contexts.push(bc.name);
        }
        let (source, target) = (contexts[0].clone(), contexts[1].clone());
        let (skilj, _) = Skilj::builder(database_url)
            // Only this test's own ticks fire deadlines: the background one
            // scans every bounded context and would race them - stopped
            // below (`dispatchers_without_background_ticks`).
            .deadline_poll_interval(std::time::Duration::from_secs(3600))
            .bounded_context(target.clone())
            .event_type::<RaceFired>()
            .command_type::<RaceCommand>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        let (command_dispatcher, projection_dispatcher, snapshot_dispatcher) =
            dispatchers_without_background_ticks(skilj).await;

        let now = test_now();
        let insert = |id: String, fire_at: chrono::DateTime<Utc>, payload: serde_json::Value| {
            let (pool, source, target) = (pool.clone(), source.clone(), target.clone());
            async move {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {}.deadlines \
                     (id, schedule_name, fire_at, tags, correlation_id, target_bounded_context, \
                      target_command_type, payload, status, created_at) \
                     VALUES ($1, 'schedule', $2, '[]'::jsonb, NULL, $3, $4, $5, 'pending', $6)",
                    race_schema(&source)
                )))
                .bind(&id)
                .bind(fire_at)
                .bind(&target)
                .bind(RaceCommand::NAME)
                .bind(payload.to_string())
                .bind(now)
                .execute(&pool)
                .await
                .unwrap();
            }
        };
        let failing = unique_name("failing");
        let valid = unique_name("valid");
        insert(
            failing.clone(),
            now - chrono::Duration::seconds(2),
            serde_json::json!({ "not_an_order_id": 1 }),
        )
        .await;
        insert(
            valid.clone(),
            now - chrono::Duration::seconds(1),
            serde_json::json!({ "order_id": "order-1" }),
        )
        .await;
        let status = |id: String| {
            let (pool, source) = (pool.clone(), source.clone());
            async move {
                let (status,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT status FROM {}.deadlines WHERE id = $1",
                    race_schema(&source)
                )))
                .bind(&id)
                .fetch_one(&pool)
                .await
                .unwrap();
                status
            }
        };

        let tick = |at: chrono::DateTime<Utc>| {
            let (pool, source) = (pool.clone(), source.clone());
            let (c, p, s) = (
                command_dispatcher.clone(),
                projection_dispatcher.clone(),
                snapshot_dispatcher.clone(),
            );
            async move {
                db::fire_due_deadlines(
                    &pool,
                    &*c,
                    &*p,
                    &*s,
                    &skilj_core::event_store::EventBroadcaster::new(16),
                    &skilj_core::event_cache::EventCache::new(0),
                    &source,
                    at,
                    None,
                    &skilj_retry::RetryPolicy {
                        initial_backoff: std::time::Duration::from_secs(60),
                        multiplier: 1.0,
                        max_backoff: std::time::Duration::from_secs(60),
                        max_attempts: Some(2),
                        max_elapsed: None,
                    },
                    &[],
                )
                .await
            }
        };

        // First attempt fails: the tick still succeeds, the valid deadline
        // behind it fires, and the failing one waits for its retry.
        tick(now).await.unwrap();
        assert_eq!(status(valid.clone()).await, "fired");
        assert_eq!(status(failing.clone()).await, "pending");

        // Not before its backoff has passed.
        tick(now + chrono::Duration::seconds(30)).await.unwrap();
        assert_eq!(status(failing.clone()).await, "pending");
        assert!(db::list_parked_deliveries(&pool, &target)
            .await
            .unwrap()
            .is_empty());

        // The second attempt exhausts the policy: parked in the target
        // context, redrivable under the deadline's own idempotency key.
        tick(now + chrono::Duration::seconds(61)).await.unwrap();
        assert_eq!(status(failing.clone()).await, "parked");
        let (payload,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT payload FROM {}.deadlines WHERE id = $1",
            race_schema(&source)
        )))
        .bind(&failing)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(payload, "{}", "a parked deadline kept its payload (§162)");
        let parked = db::list_parked_deliveries(&pool, &target).await.unwrap();
        assert_eq!(parked.len(), 1);
        let parked = &parked[0];
        assert_eq!(parked.kind, db::ParkedDeliveryKind::Deadline);
        assert_eq!(parked.identifier, failing);
        assert_eq!(parked.source, "deadline:schedule");
        assert_eq!(parked.attempt_count, 2);
        assert_eq!(
            parked.target_bounded_context.as_deref(),
            Some(target.as_str())
        );
        assert_eq!(
            parked.target_command_type.as_deref(),
            Some(RaceCommand::NAME)
        );
        assert!(parked.error.contains("order_id"), "{}", parked.error);
        assert_eq!(
            db::parked_delivery_redrive_identity(parked, "unused", None),
            Some((
                db::DEADLINE_CLIENT_ID.to_string(),
                format!("skilj-deadline:{failing}")
            ))
        );

        // Parked is final for the deadline: later ticks leave it alone.
        tick(now + chrono::Duration::hours(1)).await.unwrap();
        assert_eq!(status(failing.clone()).await, "parked");
        assert_eq!(
            db::list_parked_deliveries(&pool, &target)
                .await
                .unwrap()
                .len(),
            1
        );
    });
}

/// An instance that declares no command types at all - an older version,
/// mid rolling deploy, next to one that declares the deadline's target.
struct OlderInstance;

impl skilj_core::plugin::CommandDispatcher for OlderInstance {
    fn dispatch(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        _payload: &str,
        _matching_events: &[skilj_core::event_store::Event],
    ) -> Option<skilj_core::error::Result<skilj_core::shared::CommandDecision>> {
        None
    }
    fn required_role(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn snapshot_name(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn dispatch_from_snapshot(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        _payload: &str,
        _snapshot_state_json: &str,
        _events_since_snapshot: &[skilj_core::event_store::Event],
    ) -> Option<skilj_core::error::Result<skilj_core::shared::CommandDecision>> {
        None
    }
}

/// docs/architecture.md §161: an instance that doesn't declare a due
/// deadline's target command type leaves it for one that does - no
/// attempt spent, never parked, however often it's the one to claim it.
/// It used to count as an ordinary failure, so a rolling deploy could
/// park a deadline nothing was wrong with.
#[test]
fn a_deadline_an_instance_cannot_fire_waits_for_one_that_can() {
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
        let mut contexts = Vec::new();
        for prefix in ["source", "target"] {
            let bc = BoundedContext {
                name: unique_name(prefix),
                status: BoundedContextStatus::Active,
                created_at: test_now(),
                created_by: ContextCreator::SystemCreator,
                template: None,
            };
            db::insert_bounded_context(&pool, &bc).await.unwrap();
            db::insert_role_access_mapping(
                &pool,
                &RoleAccessMapping {
                    role: role.clone(),
                    bounded_context: bc.clone(),
                    level: AccessLevel::Admin,
                    can_read_sensitive: false,
                    scope: None,
                    status: RoleStatus::Active,
                    created_at: test_now(),
                    revoked_at: None,
                },
            )
            .await
            .unwrap();
            contexts.push(bc.name);
        }
        let (source, target) = (contexts[0].clone(), contexts[1].clone());
        let (skilj, _) = Skilj::builder(database_url)
            .deadline_poll_interval(std::time::Duration::from_secs(3600))
            .bounded_context(target.clone())
            .event_type::<RaceFired>()
            .command_type::<RaceCommand>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        let (newer, projection_dispatcher, snapshot_dispatcher) =
            dispatchers_without_background_ticks(skilj).await;
        let older: std::sync::Arc<dyn skilj_core::plugin::CommandDispatcher> =
            std::sync::Arc::new(OlderInstance);

        let now = test_now();
        let id = unique_name("deadline");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {}.deadlines \
             (id, schedule_name, fire_at, tags, correlation_id, target_bounded_context, \
              target_command_type, payload, status, created_at) \
             VALUES ($1, 'schedule', $2, '[]'::jsonb, NULL, $3, $4, $5, 'pending', $2)",
            race_schema(&source)
        )))
        .bind(&id)
        .bind(now - chrono::Duration::seconds(1))
        .bind(&target)
        .bind(RaceCommand::NAME)
        .bind(serde_json::json!({ "order_id": "order-1" }).to_string())
        .execute(&pool)
        .await
        .unwrap();
        let state = || {
            let (pool, source, id) = (pool.clone(), source.clone(), id.clone());
            async move {
                let row: (String, i32) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT status, attempt_count FROM {}.deadlines WHERE id = $1",
                    race_schema(&source)
                )))
                .bind(&id)
                .fetch_one(&pool)
                .await
                .unwrap();
                row
            }
        };
        let tick = |dispatcher: std::sync::Arc<dyn skilj_core::plugin::CommandDispatcher>,
                    at: chrono::DateTime<Utc>| {
            let (pool, source) = (pool.clone(), source.clone());
            let (p, s) = (projection_dispatcher.clone(), snapshot_dispatcher.clone());
            async move {
                db::fire_due_deadlines(
                    &pool,
                    &*dispatcher,
                    &*p,
                    &*s,
                    &skilj_core::event_store::EventBroadcaster::new(16),
                    &skilj_core::event_cache::EventCache::new(0),
                    &source,
                    at,
                    None,
                    // One attempt would park an ordinary failure at once.
                    &skilj_retry::RetryPolicy {
                        initial_backoff: std::time::Duration::from_secs(60),
                        multiplier: 1.0,
                        max_backoff: std::time::Duration::from_secs(60),
                        max_attempts: Some(1),
                        max_elapsed: None,
                    },
                    &[],
                )
                .await
                .unwrap();
            }
        };

        // The older instance claims it three times over: each time it's
        // handed back, with no attempt spent and nothing parked.
        let retry = chrono::Duration::from_std(db::ANOTHER_INSTANCE_RETRY_DELAY).unwrap();
        let mut at = now;
        for _ in 0..3 {
            tick(older.clone(), at).await;
            assert_eq!(state().await, ("pending".to_string(), 0));
            at += retry + chrono::Duration::seconds(1);
        }
        assert!(db::list_parked_deliveries(&pool, &target)
            .await
            .unwrap()
            .is_empty());

        // The instance that declares it fires it.
        tick(newer, at).await;
        assert_eq!(state().await.0, "fired");
    });
}

/// docs/architecture.md §163: `SkiljBuilder::deadline_retention` runs a
/// background task deleting resolved deadlines past it - a pending one
/// stays. Holds `ONE_DEADLINE_TEST_AT_A_TIME` and stops its `Skilj`: with a
/// one-second retention it would delete the other tests' resolved rows.
#[test]
fn resolved_deadlines_are_deleted_after_their_retention() {
    runtime().block_on(async {
        let _one_at_a_time = ONE_DEADLINE_TEST_AT_A_TIME.lock().await;
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let bc = BoundedContext {
            name: unique_name("retention"),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &bc).await.unwrap();
        let old = test_now() - chrono::Duration::hours(1);
        for (id, status, resolved_at) in
            [("fired", "fired", Some(old)), ("pending", "pending", None)]
        {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {}.deadlines \
                 (id, schedule_name, fire_at, tags, target_bounded_context, target_command_type, \
                  payload, status, created_at, resolved_at) \
                 VALUES ($1, 's', $2, '[]'::jsonb, 'x', 'C', '{{}}', $3, $2, $4)",
                race_schema(&bc.name)
            )))
            .bind(id)
            .bind(old)
            .bind(status)
            .bind(resolved_at)
            .execute(&pool)
            .await
            .unwrap();
        }

        let (skilj, _) = Skilj::builder(database_url)
            .deadline_poll_interval(std::time::Duration::from_secs(3600))
            .deadline_retention(std::time::Duration::from_secs(1))
            .build()
            .await
            .unwrap();
        let ids = || {
            let (pool, bc_name) = (pool.clone(), bc.name.clone());
            async move {
                let mut ids: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT id FROM {}.deadlines",
                    race_schema(&bc_name)
                )))
                .fetch_all(&pool)
                .await
                .unwrap();
                ids.sort();
                ids
            }
        };
        let mut remaining = ids().await;
        for _ in 0..100 {
            if remaining == vec!["pending".to_string()] {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            remaining = ids().await;
        }
        skilj.shutdown(std::time::Duration::from_secs(20)).await;
        assert_eq!(remaining, vec!["pending".to_string()]);
    });
}
