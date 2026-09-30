//! End-to-end proof of `CrossContextRoute` (this crate's own answer to
//! "make messages cross bounded contexts without needing an external
//! system like Temporal" - see `skilj_core::plugin::CrossContextRoute`'s
//! own doc comment): a real `OrderShipped` event, directly created in one
//! bounded context ("shipping"), is picked up by the background poll task
//! `SkiljBuilder::build()` spawns for it and turned into a real
//! `ReserveStock` command submission in a *different* bounded context
//! ("inventory") - proof the wiring from `skilj_core::db::
//! catch_up_cross_context_route` all the way through `SkiljBuilder::
//! cross_context_route::<R>()` is real and running, not just the
//! persistence layer in isolation. Same `DATABASE_URL`-then-embedded-
//! Postgres-then-skip harness as `skilj/tests/async_projections.rs` - see
//! its own doc comment for the details, not repeated a third time here.
//!
//! Two bounded contexts in one process, like `skilj/tests/
//! event_subscription.rs`'s multi-bc tests - but unlike those, `Source`/
//! `Target` here must each carry their own `BOUNDED_CONTEXT` const
//! override matching the name each is registered under, since
//! `SkiljBuilder::cross_context_route::<R>()` derives `source_bounded_context`/
//! `target_bounded_context` from `R::Source::BOUNDED_CONTEXT`/
//! `R::Target::BOUNDED_CONTEXT` directly - never from the builder's own
//! "current bounded context" chain `.event_type::<T>()`/`.command_type::<T>()`
//! use (see `SkiljBuilder::cross_context_route`'s own doc comment).
//! Fixed, literal bounded context names, not `unique_name()` - the same
//! precedent `skilj/tests/auto_register.rs`'s `CUSTOM_BOUNDED_CONTEXT`
//! sets, since a `const` can't hold a value only known at runtime; unique
//! per test *binary* (its own embedded/DATABASE_URL Postgres instance),
//! which is all that's required here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, CrossContextRoute, EventType, Projection, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{event_causation_id, BoundedContext, BoundedContextStatus};
use skilj_core::plugin::{BoundedContextEvent, CrossContextRouteStartFrom};
use skilj_core::shared::{generate_token_id, generate_token_secret, CommandDecision, EventSpec};
use tower::ServiceExt;

// --- shipping bounded context (route source) ---

const SHIPPING_BOUNDED_CONTEXT: &str = "skilj_cross_context_route_test_shipping";
const INVENTORY_BOUNDED_CONTEXT: &str = "skilj_cross_context_route_test_inventory";

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OrderShippedPayload {
    order_id: String,
    quantity: i64,
    /// Exercises `route()` returning `None` (see `ShippingToInventory::route`
    /// below) - the occurrence this route itself decided doesn't apply,
    /// still advancing the cursor with no `ReserveStock` submitted.
    backorder: bool,
}

struct OrderShipped;

impl EventType for OrderShipped {
    type Payload = OrderShippedPayload;
    const NAME: &'static str = "OrderShipped";
    const BOUNDED_CONTEXT: &'static str = SHIPPING_BOUNDED_CONTEXT;
    fn direct_creation_allowed() -> bool {
        true
    }
}

// --- inventory bounded context (route target) ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct ReserveStockPayload {
    order_id: String,
    quantity: i64,
}

struct ReserveStock;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct StockReservedPayload {
    order_id: String,
    quantity: i64,
}

struct StockReserved;

impl EventType for StockReserved {
    type Payload = StockReservedPayload;
    const NAME: &'static str = "StockReserved";
    const BOUNDED_CONTEXT: &'static str = INVENTORY_BOUNDED_CONTEXT;
}

enum InventoryEvent {
    StockReserved(StockReservedPayload),
}

impl BoundedContextEvent for InventoryEvent {
    fn try_from_event(
        event: &skilj_core::event_store::Event,
    ) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "StockReserved" => {
                Some(serde_json::from_str(&event.payload).map(InventoryEvent::StockReserved))
            }
            _ => None,
        }
    }
}

impl CommandType for ReserveStock {
    type Payload = ReserveStockPayload;
    type Event = InventoryEvent;
    const NAME: &'static str = "ReserveStock";
    const BOUNDED_CONTEXT: &'static str = INVENTORY_BOUNDED_CONTEXT;
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "StockReserved".to_string(),
                payload: serde_json::json!({
                    "order_id": payload.order_id,
                    "quantity": payload.quantity,
                }),
            }],
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct ReservedTotalState {
    total: i64,
}

/// Overrides `sync()` to `true` - the assertion below is already only
/// waiting on the *route's* own poll task; there's no reason to also make
/// it wait on a second, independent projection poll task on top.
struct ReservedTotal;

impl Projection for ReservedTotal {
    type State = ReservedTotalState;
    type Event = InventoryEvent;
    const NAME: &'static str = "ReservedTotal";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["StockReserved"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        let InventoryEvent::StockReserved(payload) = event;
        state.total += payload.quantity;
    }
}

// --- the route itself ---

struct ShippingToInventory;

impl CrossContextRoute for ShippingToInventory {
    type Source = OrderShipped;
    type Target = ReserveStock;
    const NAME: &'static str = "ShippingToInventory";
    fn route(source_payload: &OrderShippedPayload) -> Option<ReserveStockPayload> {
        if source_payload.backorder {
            return None;
        }
        Some(ReserveStockPayload {
            order_id: source_payload.order_id.clone(),
            quantity: source_payload.quantity,
        })
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for cross_context_route tests")
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
    let database_url = if let Ok(database_url) = std::env::var("DATABASE_URL") {
        database_url
    } else {
        let url = skilj_test_support::database_url("skilj_cross_context_route_e2e_test").await?;
        let pool = connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            pool,
        });
    };

    let pool = connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb { database_url, pool })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

#[test]
fn an_event_in_one_bounded_context_eventually_submits_a_command_in_another() {
    runtime().block_on(async {
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

        let shipping_bc = BoundedContext {
            name: SHIPPING_BOUNDED_CONTEXT.to_string(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &shipping_bc)
            .await
            .unwrap();
        let inventory_bc = BoundedContext {
            name: INVENTORY_BOUNDED_CONTEXT.to_string(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &inventory_bc)
            .await
            .unwrap();

        let shipping_mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: shipping_bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &shipping_mapping)
            .await
            .unwrap();
        let inventory_mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: inventory_bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &inventory_mapping)
            .await
            .unwrap();

        let (skilj, report) = Skilj::builder(database_url)
            .bounded_context(SHIPPING_BOUNDED_CONTEXT)
            .event_type::<OrderShipped>()
            .bounded_context(INVENTORY_BOUNDED_CONTEXT)
            .event_type::<StockReserved>()
            .command_type::<ReserveStock>()
            .projection::<ReservedTotal>()
            .cross_context_route::<ShippingToInventory>()
            .cross_context_route_poll_interval(std::time::Duration::from_millis(50))
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let event_type = db::get_event_type(&pool, SHIPPING_BOUNDED_CONTEXT, "OrderShipped")
            .await
            .unwrap()
            .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            &shipping_mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let credential = format!("{}.{}", direct_token.id, direct_token.secret);

        let router = skilj.rest_router();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"payload":{"order_id":"order-1","quantity":7,"backorder":false}}"#,
            ))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["sequence"], 0);

        // The response above only confirms the shipping-side event was
        // written - proof the route itself fired lives on the inventory
        // side, reached only once the route's own 50ms poll task has run
        // at least once; give it a few ticks' worth of margin rather than
        // relying on exactly one.
        let mut state = None;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            state = db::get_projection_state(&pool, INVENTORY_BOUNDED_CONTEXT, "ReservedTotal", "")
                .await
                .unwrap();
            if state.as_deref() == Some(r#"{"total":7}"#) {
                break;
            }
        }
        assert_eq!(state, Some(r#"{"total":7}"#.to_string()));

        // Codeberg issue #18 - the routed `ReserveStock` command finally
        // has a real answer to "what caused this": its correlation_id is
        // forward-carried from the source `OrderShipped` event (itself
        // generated, since the direct-event submission above supplied
        // none), and its causation_id names that exact source event via
        // `event_causation_id`'s composed `{bounded_context}:{sequence}`
        // string.
        let source_event = db::get_event_by_sequence(&pool, SHIPPING_BOUNDED_CONTEXT, 0)
            .await
            .unwrap()
            .unwrap();
        assert!(source_event.metadata.correlation_id.is_some());
        let routed_command =
            db::list_commands_for_bounded_context(&pool, INVENTORY_BOUNDED_CONTEXT)
                .await
                .unwrap()
                .into_iter()
                .find(|c| c.command_type.name == "ReserveStock")
                .expect("the route must have submitted a real ReserveStock command by now");
        assert_eq!(
            routed_command.metadata.correlation_id,
            source_event.metadata.correlation_id
        );
        assert_eq!(
            routed_command.metadata.causation_id,
            Some(event_causation_id(&source_event))
        );

        // A second `OrderShipped` occurrence with `backorder: true` -
        // `route()` returns `None` for it (this fixture's own stand-in
        // for "this occurrence doesn't apply"), so no `ReserveStock`
        // ever gets submitted for it.
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"payload":{"order_id":"order-2","quantity":3,"backorder":true}}"#,
            ))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["sequence"], 1);

        // Give the route several poll intervals' worth of margin, then
        // confirm the total is still exactly what the first event alone
        // produced - a skipped occurrence must never submit a command.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let state = db::get_projection_state(&pool, INVENTORY_BOUNDED_CONTEXT, "ReservedTotal", "")
            .await
            .unwrap();
        assert_eq!(state, Some(r#"{"total":7}"#.to_string()));

        // A third, ordinary occurrence - proof the skip above didn't
        // stall the route's own cursor: if it had, this would never be
        // reached either.
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"payload":{"order_id":"order-3","quantity":5,"backorder":false}}"#,
            ))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["sequence"], 2);

        let mut state = None;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            state = db::get_projection_state(&pool, INVENTORY_BOUNDED_CONTEXT, "ReservedTotal", "")
                .await
                .unwrap();
            if state.as_deref() == Some(r#"{"total":12}"#) {
                break;
            }
        }
        assert_eq!(state, Some(r#"{"total":12}"#.to_string()));
    });
}

// --- CrossContextRoute::START_FROM = Latest: the "don't email every
// user who has ever registered" scenario ---

const SHIPPING_BOUNDED_CONTEXT_LATEST: &str = "skilj_cross_context_route_test_shipping_latest";
const INVENTORY_BOUNDED_CONTEXT_LATEST: &str = "skilj_cross_context_route_test_inventory_latest";

struct OrderShippedLatest;

impl EventType for OrderShippedLatest {
    type Payload = OrderShippedPayload;
    const NAME: &'static str = "OrderShipped";
    const BOUNDED_CONTEXT: &'static str = SHIPPING_BOUNDED_CONTEXT_LATEST;
    fn direct_creation_allowed() -> bool {
        true
    }
}

struct ReserveStockLatest;

impl CommandType for ReserveStockLatest {
    type Payload = ReserveStockPayload;
    type Event = InventoryEvent;
    const NAME: &'static str = "ReserveStock";
    const BOUNDED_CONTEXT: &'static str = INVENTORY_BOUNDED_CONTEXT_LATEST;
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "StockReserved".to_string(),
                payload: serde_json::json!({
                    "order_id": payload.order_id,
                    "quantity": payload.quantity,
                }),
            }],
        }
    }
}

struct StockReservedLatest;

impl EventType for StockReservedLatest {
    type Payload = StockReservedPayload;
    const NAME: &'static str = "StockReserved";
    const BOUNDED_CONTEXT: &'static str = INVENTORY_BOUNDED_CONTEXT_LATEST;
}

struct ReservedTotalLatest;

impl Projection for ReservedTotalLatest {
    type State = ReservedTotalState;
    type Event = InventoryEvent;
    const NAME: &'static str = "ReservedTotal";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["StockReserved"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        let InventoryEvent::StockReserved(payload) = event;
        state.total += payload.quantity;
    }
}

struct ShippingToInventoryLatest;

impl CrossContextRoute for ShippingToInventoryLatest {
    type Source = OrderShippedLatest;
    type Target = ReserveStockLatest;
    const NAME: &'static str = "ShippingToInventoryLatest";
    const START_FROM: CrossContextRouteStartFrom = CrossContextRouteStartFrom::Latest;
    fn route(source_payload: &OrderShippedPayload) -> Option<ReserveStockPayload> {
        Some(ReserveStockPayload {
            order_id: source_payload.order_id.clone(),
            quantity: source_payload.quantity,
        })
    }
}

/// The scenario this feature exists for: an `OrderShipped` occurrence
/// committed *before* `ShippingToInventoryLatest` is ever registered must
/// never be dispatched, even though nothing but this route's own
/// `START_FROM` differs from the ordinary `Beginning` test above. Two
/// `Skilj::builder()` calls against the same two bounded contexts, not
/// one - the route is deliberately absent from the first (registering
/// `OrderShippedLatest`/`ReserveStockLatest`/`StockReservedLatest`/
/// `ReservedTotalLatest` alone, exactly enough to mint a token and post
/// one event through), then present in the second, so the "already
/// existed at registration time" the route needs to skip is genuine
/// pre-existing history, not a race against the route's own first tick.
#[test]
fn a_latest_route_never_dispatches_history_that_predates_its_own_registration() {
    runtime().block_on(async {
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

        let shipping_bc = BoundedContext {
            name: SHIPPING_BOUNDED_CONTEXT_LATEST.to_string(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &shipping_bc)
            .await
            .unwrap();
        let inventory_bc = BoundedContext {
            name: INVENTORY_BOUNDED_CONTEXT_LATEST.to_string(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &inventory_bc)
            .await
            .unwrap();

        let shipping_mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: shipping_bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &shipping_mapping)
            .await
            .unwrap();
        let inventory_mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: inventory_bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &inventory_mapping)
            .await
            .unwrap();

        // First build: no route at all yet, just enough to mint a token
        // and post one "historical" OrderShipped occurrence.
        let (skilj, report) = Skilj::builder(database_url.clone())
            .bounded_context(SHIPPING_BOUNDED_CONTEXT_LATEST)
            .event_type::<OrderShippedLatest>()
            .bounded_context(INVENTORY_BOUNDED_CONTEXT_LATEST)
            .event_type::<StockReservedLatest>()
            .command_type::<ReserveStockLatest>()
            .projection::<ReservedTotalLatest>()
            .reconciliation_role(external_subject.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let event_type = db::get_event_type(&pool, SHIPPING_BOUNDED_CONTEXT_LATEST, "OrderShipped")
            .await
            .unwrap()
            .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            &shipping_mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let credential = format!("{}.{}", direct_token.id, direct_token.secret);

        let router = skilj.rest_router();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"payload":{"order_id":"order-historical","quantity":7,"backorder":false}}"#,
            ))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Second build, same two bounded contexts: this time the route
        // is registered too, `START_FROM: Latest`. Bound but otherwise
        // unused - the route's own background poll task needs this
        // second `Skilj` kept alive for the rest of the test, but every
        // request below still goes through the first build's own
        // `router` (both point at the same two bounded contexts).
        let (_skilj, report) = Skilj::builder(database_url)
            .bounded_context(SHIPPING_BOUNDED_CONTEXT_LATEST)
            .event_type::<OrderShippedLatest>()
            .bounded_context(INVENTORY_BOUNDED_CONTEXT_LATEST)
            .event_type::<StockReservedLatest>()
            .command_type::<ReserveStockLatest>()
            .projection::<ReservedTotalLatest>()
            .cross_context_route::<ShippingToInventoryLatest>()
            .cross_context_route_poll_interval(std::time::Duration::from_millis(50))
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        // Several poll intervals' worth of margin, then confirm the
        // historical occurrence was never dispatched - if it had been,
        // this would already read {"total":7}.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let state =
            db::get_projection_state(&pool, INVENTORY_BOUNDED_CONTEXT_LATEST, "ReservedTotal", "")
                .await
                .unwrap();
        assert_ne!(state, Some(r#"{"total":7}"#.to_string()));

        // A genuinely new occurrence, posted after the route exists,
        // still gets dispatched exactly like a Beginning route's would.
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"payload":{"order_id":"order-new","quantity":3,"backorder":false}}"#,
            ))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let mut state = None;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            state = db::get_projection_state(
                &pool,
                INVENTORY_BOUNDED_CONTEXT_LATEST,
                "ReservedTotal",
                "",
            )
            .await
            .unwrap();
            if state.as_deref() == Some(r#"{"total":3}"#) {
                break;
            }
        }
        // Exactly 3, not 10 - the historical 7 must never count, even
        // once the route is fully caught up and running normally.
        assert_eq!(state, Some(r#"{"total":3}"#.to_string()));
    });
}

// --- CrossContextRoute::START_FROM = AtSequence/AtTime: replaying from
// a chosen mid-history point, not just "none" (Latest) or "everything"
// (Beginning) ---

const SHIPPING_BOUNDED_CONTEXT_CUTOFFS: &str = "skilj_cross_context_route_test_shipping_cutoffs";
const INVENTORY_BOUNDED_CONTEXT_CUTOFFS: &str = "skilj_cross_context_route_test_inventory_cutoffs";

struct OrderShippedCutoffs;

impl EventType for OrderShippedCutoffs {
    type Payload = OrderShippedPayload;
    const NAME: &'static str = "OrderShipped";
    const BOUNDED_CONTEXT: &'static str = SHIPPING_BOUNDED_CONTEXT_CUTOFFS;
    fn direct_creation_allowed() -> bool {
        true
    }
}

// One target CommandType/EventType/Projection per route, both hosted in
// the same INVENTORY_BOUNDED_CONTEXT_CUTOFFS - two routes independently
// cursored against the same OrderShippedCutoffs stream, each dispatching
// to its own command type so neither route's own count is polluted by
// the other's.

// `InventoryEvent` (defined above, matching the literal name
// "StockReserved") can't be reused here - unlike the `Latest` scenario's
// own `StockReservedLatest`, which gets away with keeping the literal
// name "StockReserved" because it lives in its own separate bounded
// context, `StockReservedAtSequence`/`StockReservedAtTime` below share
// *one* bounded context with each other, so each needs its own distinct
// name, and therefore its own `BoundedContextEvent` impl matching it.

enum InventoryEventAtSequence {
    StockReserved(StockReservedPayload),
}

impl BoundedContextEvent for InventoryEventAtSequence {
    fn try_from_event(
        event: &skilj_core::event_store::Event,
    ) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "StockReservedAtSequence" => Some(
                serde_json::from_str(&event.payload).map(InventoryEventAtSequence::StockReserved),
            ),
            _ => None,
        }
    }
}

struct ReserveStockAtSequence;

impl CommandType for ReserveStockAtSequence {
    type Payload = ReserveStockPayload;
    type Event = InventoryEventAtSequence;
    const NAME: &'static str = "ReserveStockAtSequence";
    const BOUNDED_CONTEXT: &'static str = INVENTORY_BOUNDED_CONTEXT_CUTOFFS;
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "StockReservedAtSequence".to_string(),
                payload: serde_json::json!({
                    "order_id": payload.order_id,
                    "quantity": payload.quantity,
                }),
            }],
        }
    }
}

struct StockReservedAtSequence;

impl EventType for StockReservedAtSequence {
    type Payload = StockReservedPayload;
    const NAME: &'static str = "StockReservedAtSequence";
    const BOUNDED_CONTEXT: &'static str = INVENTORY_BOUNDED_CONTEXT_CUTOFFS;
}

struct ReservedTotalAtSequence;

impl Projection for ReservedTotalAtSequence {
    type State = ReservedTotalState;
    type Event = InventoryEventAtSequence;
    const NAME: &'static str = "ReservedTotalAtSequence";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["StockReservedAtSequence"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        let InventoryEventAtSequence::StockReserved(payload) = event;
        state.total += payload.quantity;
    }
}

struct RouteAtSequence;

impl CrossContextRoute for RouteAtSequence {
    type Source = OrderShippedCutoffs;
    type Target = ReserveStockAtSequence;
    const NAME: &'static str = "RouteAtSequence";
    const START_FROM: CrossContextRouteStartFrom = CrossContextRouteStartFrom::AtSequence(0);
    fn route(source_payload: &OrderShippedPayload) -> Option<ReserveStockPayload> {
        Some(ReserveStockPayload {
            order_id: source_payload.order_id.clone(),
            quantity: source_payload.quantity,
        })
    }
}

enum InventoryEventAtTime {
    StockReserved(StockReservedPayload),
}

impl BoundedContextEvent for InventoryEventAtTime {
    fn try_from_event(
        event: &skilj_core::event_store::Event,
    ) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "StockReservedAtTime" => {
                Some(serde_json::from_str(&event.payload).map(InventoryEventAtTime::StockReserved))
            }
            _ => None,
        }
    }
}

struct ReserveStockAtTime;

impl CommandType for ReserveStockAtTime {
    type Payload = ReserveStockPayload;
    type Event = InventoryEventAtTime;
    const NAME: &'static str = "ReserveStockAtTime";
    const BOUNDED_CONTEXT: &'static str = INVENTORY_BOUNDED_CONTEXT_CUTOFFS;
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "StockReservedAtTime".to_string(),
                payload: serde_json::json!({
                    "order_id": payload.order_id,
                    "quantity": payload.quantity,
                }),
            }],
        }
    }
}

struct StockReservedAtTime;

impl EventType for StockReservedAtTime {
    type Payload = StockReservedPayload;
    const NAME: &'static str = "StockReservedAtTime";
    const BOUNDED_CONTEXT: &'static str = INVENTORY_BOUNDED_CONTEXT_CUTOFFS;
}

struct ReservedTotalAtTime;

impl Projection for ReservedTotalAtTime {
    type State = ReservedTotalState;
    type Event = InventoryEventAtTime;
    const NAME: &'static str = "ReservedTotalAtTime";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["StockReservedAtTime"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        let InventoryEventAtTime::StockReserved(payload) = event;
        state.total += payload.quantity;
    }
}

/// `START_FROM: AtTime(unix_secs)` - the const has to be known at
/// compile time, so the cutoff itself is baked in as a fixed point
/// safely in the past (well before this test binary's own run), and the
/// proof of "replays from a chosen point, not from nothing" instead
/// comes from `RouteAtSequence`'s own test below sharing this same
/// pattern with a caller-chosen sequence - `AtTime`'s own mid-history
/// behaviour is already proven for real over REST
/// (`skilj/tests/event_fetch_rest.rs::an_at_time_token_replays_history_after_a_chosen_cutoff_but_not_before_it`),
/// so this route only needs to prove the *seeding mechanism itself*
/// runs for `AtTime` the same way it does for `AtSequence`/`Latest` -
/// nothing committed before this fixed cutoff, so every deposit in this
/// test counts, the same as `Beginning` would give, which is exactly
/// what the spec's own "empty means the whole stream" note on
/// `at_time_position` predicts.
struct RouteAtTime;

impl CrossContextRoute for RouteAtTime {
    type Source = OrderShippedCutoffs;
    type Target = ReserveStockAtTime;
    const NAME: &'static str = "RouteAtTime";
    const START_FROM: CrossContextRouteStartFrom = CrossContextRouteStartFrom::AtTime(0);
    fn route(source_payload: &OrderShippedPayload) -> Option<ReserveStockPayload> {
        Some(ReserveStockPayload {
            order_id: source_payload.order_id.clone(),
            quantity: source_payload.quantity,
        })
    }
}

/// Both routes react to the same `OrderShippedCutoffs` stream, each with
/// its own independent cursor (`cross_context_route_cursors` keyed by
/// `route_name`) and its own target command/event/projection, so
/// neither's own count is polluted by the other's - proof two routes
/// with different `START_FROM` values can coexist against one source
/// without interfering.
///
/// Two `Skilj::builder()` calls, same shape as the `Latest` test above:
/// the first registers only the source/target types and posts one
/// "historical" `OrderShipped` occurrence at sequence zero; the second
/// adds both routes. `RouteAtSequence`'s own `AtSequence(0)` cutoff sits
/// exactly at that historical occurrence's own sequence, so a second
/// occurrence posted before either route is ever registered, at
/// sequence one, still gets replayed once `RouteAtSequence` starts - the
/// concrete proof this mechanism can replay *some* history, not just
/// none at all the way `Latest` can. `RouteAtTime`'s fixed, safely-past
/// `AtTime(0)` cutoff proves the identical seeding mechanism runs for
/// that variant too (see its own doc comment above for why its
/// mid-history behaviour specifically is proven over REST instead, not
/// here).
#[test]
fn at_sequence_and_at_time_routes_replay_from_their_own_chosen_cutoffs() {
    runtime().block_on(async {
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

        let shipping_bc = BoundedContext {
            name: SHIPPING_BOUNDED_CONTEXT_CUTOFFS.to_string(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &shipping_bc)
            .await
            .unwrap();
        let inventory_bc = BoundedContext {
            name: INVENTORY_BOUNDED_CONTEXT_CUTOFFS.to_string(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &inventory_bc)
            .await
            .unwrap();

        let shipping_mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: shipping_bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &shipping_mapping)
            .await
            .unwrap();
        let inventory_mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: inventory_bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &inventory_mapping)
            .await
            .unwrap();

        // First build: no routes yet, just enough to mint a token and
        // post two "historical" OrderShipped occurrences.
        let (skilj, report) = Skilj::builder(database_url.clone())
            .bounded_context(SHIPPING_BOUNDED_CONTEXT_CUTOFFS)
            .event_type::<OrderShippedCutoffs>()
            .bounded_context(INVENTORY_BOUNDED_CONTEXT_CUTOFFS)
            .event_type::<StockReservedAtSequence>()
            .command_type::<ReserveStockAtSequence>()
            .projection::<ReservedTotalAtSequence>()
            .event_type::<StockReservedAtTime>()
            .command_type::<ReserveStockAtTime>()
            .projection::<ReservedTotalAtTime>()
            .reconciliation_role(external_subject.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let event_type =
            db::get_event_type(&pool, SHIPPING_BOUNDED_CONTEXT_CUTOFFS, "OrderShipped")
                .await
                .unwrap()
                .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            &shipping_mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let credential = format!("{}.{}", direct_token.id, direct_token.secret);

        let router = skilj.rest_router();
        // Sequence 0 - RouteAtSequence's own cutoff sits exactly here.
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"payload":{"order_id":"order-0","quantity":5,"backorder":false}}"#,
            ))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        // Sequence 1 - historical relative to either route's own
        // registration below, but after RouteAtSequence's cutoff.
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"payload":{"order_id":"order-1","quantity":7,"backorder":false}}"#,
            ))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        // Second build, same bounded contexts: both routes registered
        // this time.
        let (_skilj, report) = Skilj::builder(database_url)
            .bounded_context(SHIPPING_BOUNDED_CONTEXT_CUTOFFS)
            .event_type::<OrderShippedCutoffs>()
            .bounded_context(INVENTORY_BOUNDED_CONTEXT_CUTOFFS)
            .event_type::<StockReservedAtSequence>()
            .command_type::<ReserveStockAtSequence>()
            .projection::<ReservedTotalAtSequence>()
            .event_type::<StockReservedAtTime>()
            .command_type::<ReserveStockAtTime>()
            .projection::<ReservedTotalAtTime>()
            .cross_context_route::<RouteAtSequence>()
            .cross_context_route::<RouteAtTime>()
            .cross_context_route_poll_interval(std::time::Duration::from_millis(50))
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        // RouteAtSequence: sequence 0 stays unserved (at the cutoff),
        // sequence 1 - historical, but after the cutoff - gets replayed.
        let mut at_sequence_state = None;
        // RouteAtTime: nothing existed before its own fixed, safely-past
        // cutoff, so both occurrences count - the "empty means the whole
        // stream" case.
        let mut at_time_state = None;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            at_sequence_state = db::get_projection_state(
                &pool,
                INVENTORY_BOUNDED_CONTEXT_CUTOFFS,
                "ReservedTotalAtSequence",
                "",
            )
            .await
            .unwrap();
            at_time_state = db::get_projection_state(
                &pool,
                INVENTORY_BOUNDED_CONTEXT_CUTOFFS,
                "ReservedTotalAtTime",
                "",
            )
            .await
            .unwrap();
            if at_sequence_state.as_deref() == Some(r#"{"total":7}"#)
                && at_time_state.as_deref() == Some(r#"{"total":12}"#)
            {
                break;
            }
        }
        assert_eq!(
            at_sequence_state,
            Some(r#"{"total":7}"#.to_string()),
            "AtSequence(0) must skip sequence 0 but replay sequence 1"
        );
        assert_eq!(
            at_time_state,
            Some(r#"{"total":12}"#.to_string()),
            "AtTime's fixed past cutoff must replay everything, like Beginning would"
        );

        // A genuinely new occurrence, posted after both routes exist -
        // both must still react to it normally.
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"payload":{"order_id":"order-2","quantity":100,"backorder":false}}"#,
            ))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let mut at_sequence_state = None;
        let mut at_time_state = None;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            at_sequence_state = db::get_projection_state(
                &pool,
                INVENTORY_BOUNDED_CONTEXT_CUTOFFS,
                "ReservedTotalAtSequence",
                "",
            )
            .await
            .unwrap();
            at_time_state = db::get_projection_state(
                &pool,
                INVENTORY_BOUNDED_CONTEXT_CUTOFFS,
                "ReservedTotalAtTime",
                "",
            )
            .await
            .unwrap();
            if at_sequence_state.as_deref() == Some(r#"{"total":107}"#)
                && at_time_state.as_deref() == Some(r#"{"total":112}"#)
            {
                break;
            }
        }
        assert_eq!(at_sequence_state, Some(r#"{"total":107}"#.to_string()));
        assert_eq!(at_time_state, Some(r#"{"total":112}"#.to_string()));
    });
}

// --- more routes than pool connections (docs/architecture.md §117) ---

const POOL_SOURCE_BOUNDED_CONTEXT: &str = "skilj_cross_context_route_test_pool_source";
const POOL_TARGET_BOUNDED_CONTEXT: &str = "skilj_cross_context_route_test_pool_target";

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct TicketSoldPayload {
    ticket: String,
}

struct TicketSold;

impl EventType for TicketSold {
    type Payload = TicketSoldPayload;
    const NAME: &'static str = "TicketSold";
    const BOUNDED_CONTEXT: &'static str = POOL_SOURCE_BOUNDED_CONTEXT;
    fn direct_creation_allowed() -> bool {
        true
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct SeatHeldPayload {
    route: String,
}

struct SeatHeld;

impl EventType for SeatHeld {
    type Payload = SeatHeldPayload;
    const NAME: &'static str = "SeatHeld";
    const BOUNDED_CONTEXT: &'static str = POOL_TARGET_BOUNDED_CONTEXT;
}

enum SeatEvent {}

impl BoundedContextEvent for SeatEvent {
    fn try_from_event(
        _event: &skilj_core::event_store::Event,
    ) -> Option<Result<Self, serde_json::Error>> {
        None
    }
}

struct HoldSeat;

impl CommandType for HoldSeat {
    type Payload = SeatHeldPayload;
    type Event = SeatEvent;
    const NAME: &'static str = "HoldSeat";
    const BOUNDED_CONTEXT: &'static str = POOL_TARGET_BOUNDED_CONTEXT;
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "SeatHeld".to_string(),
                payload: serde_json::json!({ "route": payload.route }),
            }],
        }
    }
}

macro_rules! seat_routes {
    ($($route:ident),*) => {$(
        struct $route;

        impl CrossContextRoute for $route {
            type Source = TicketSold;
            type Target = HoldSeat;
            const NAME: &'static str = stringify!($route);
            fn route(_source_payload: &TicketSoldPayload) -> Option<SeatHeldPayload> {
                Some(SeatHeldPayload {
                    route: stringify!($route).to_string(),
                })
            }
        }
    )*};
}

seat_routes!(SeatRouteA, SeatRouteB, SeatRouteC, SeatRouteD);

/// Each route tick holds its advisory lock on a dedicated connection for
/// the whole tick, and its work needs connections of its own. Up to 16
/// ticks ran at once, so with as many routes as the pool had connections,
/// every connection was a tick's lock waiting for a second one: no route
/// ever fired, and the pool starved everything else too. Here four routes
/// share a 3-connection pool and must all fire.
#[test]
fn more_routes_than_pool_connections_all_fire() {
    runtime().block_on(async {
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
        let mut source_mapping = None;
        for name in [POOL_SOURCE_BOUNDED_CONTEXT, POOL_TARGET_BOUNDED_CONTEXT] {
            let bc = BoundedContext {
                name: name.to_string(),
                status: BoundedContextStatus::Active,
                created_at: test_now(),
                created_by: ContextCreator::SystemCreator,
                template: None,
            };
            db::insert_bounded_context(&pool, &bc).await.unwrap();
            let mapping = RoleAccessMapping {
                role: role.clone(),
                bounded_context: bc,
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            };
            db::insert_role_access_mapping(&pool, &mapping)
                .await
                .unwrap();
            source_mapping.get_or_insert(mapping);
        }

        let (skilj, report) = Skilj::builder(database_url)
            .pool_options(
                skilj_core::db::PgPoolOptions::new()
                    .max_connections(3)
                    .acquire_timeout(std::time::Duration::from_secs(5)),
            )
            .bounded_context(POOL_SOURCE_BOUNDED_CONTEXT)
            .event_type::<TicketSold>()
            .bounded_context(POOL_TARGET_BOUNDED_CONTEXT)
            .event_type::<SeatHeld>()
            .command_type::<HoldSeat>()
            .cross_context_route::<SeatRouteA>()
            .cross_context_route::<SeatRouteB>()
            .cross_context_route::<SeatRouteC>()
            .cross_context_route::<SeatRouteD>()
            .cross_context_route_poll_interval(std::time::Duration::from_millis(50))
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let event_type = db::get_event_type(&pool, POOL_SOURCE_BOUNDED_CONTEXT, "TicketSold")
            .await
            .unwrap()
            .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            source_mapping.as_ref().unwrap(),
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header(
                "authorization",
                format!("Bearer {}.{}", direct_token.id, direct_token.secret),
            )
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"ticket":"t-1"}}"#))
            .unwrap();
        let response = skilj.rest_router().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let mut routed = Vec::new();
        for _ in 0..120 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            routed = db::list_events_for_bounded_context(&pool, POOL_TARGET_BOUNDED_CONTEXT)
                .await
                .unwrap()
                .into_iter()
                .map(|e| {
                    serde_json::from_str::<serde_json::Value>(&e.payload).unwrap()["route"]
                        .as_str()
                        .unwrap()
                        .to_string()
                })
                .collect::<Vec<_>>();
            if routed.len() == 4 {
                break;
            }
        }
        routed.sort();
        assert_eq!(
            routed,
            ["SeatRouteA", "SeatRouteB", "SeatRouteC", "SeatRouteD"],
            "every route must fire within 6 s"
        );
    });
}

// --- Skilj::shutdown (docs/architecture.md §123) ---

const SHUTDOWN_SOURCE_BOUNDED_CONTEXT: &str = "skilj_cross_context_route_test_shutdown_source";
const SHUTDOWN_TARGET_BOUNDED_CONTEXT: &str = "skilj_cross_context_route_test_shutdown_target";

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct ParcelPayload {
    parcel: String,
}

struct ParcelSent;

impl EventType for ParcelSent {
    type Payload = ParcelPayload;
    const NAME: &'static str = "ParcelSent";
    const BOUNDED_CONTEXT: &'static str = SHUTDOWN_SOURCE_BOUNDED_CONTEXT;
    fn direct_creation_allowed() -> bool {
        true
    }
}

struct LabelPrinted;

impl EventType for LabelPrinted {
    type Payload = ParcelPayload;
    const NAME: &'static str = "LabelPrinted";
    const BOUNDED_CONTEXT: &'static str = SHUTDOWN_TARGET_BOUNDED_CONTEXT;
}

struct PrintLabel;

impl CommandType for PrintLabel {
    type Payload = ParcelPayload;
    type Event = SeatEvent;
    const NAME: &'static str = "PrintLabel";
    const BOUNDED_CONTEXT: &'static str = SHUTDOWN_TARGET_BOUNDED_CONTEXT;
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "LabelPrinted".to_string(),
                payload: serde_json::json!({ "parcel": payload.parcel }),
            }],
        }
    }
}

struct ParcelsToLabels;

impl CrossContextRoute for ParcelsToLabels {
    type Source = ParcelSent;
    type Target = PrintLabel;
    const NAME: &'static str = "ParcelsToLabels";
    fn route(source_payload: &ParcelPayload) -> Option<ParcelPayload> {
        Some(ParcelPayload {
            parcel: source_payload.parcel.clone(),
        })
    }
}

/// `Skilj::shutdown` stops every background loop after the tick it is
/// in, aborts what is still busy at the timeout, and dropping a `Skilj`
/// stops nothing (docs/architecture.md §123). A route's command is held
/// up by holding its target's sequence lock from outside:
/// 1. shut down while a tick waits on it, then released: the tick
///    completes - the label is printed - and every loop reports stopped;
/// 2. shut down with a short timeout while it waits: the route loop is
///    aborted and nothing is written;
/// 3. a `Skilj` dropped (its router kept) still routes - the parcel the
///    aborted tick left behind is picked up.
#[test]
fn shutdown_finishes_ticks_aborts_at_the_timeout_and_drop_stops_nothing() {
    runtime().block_on(async {
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
        let mut source_mapping = None;
        for name in [SHUTDOWN_SOURCE_BOUNDED_CONTEXT, SHUTDOWN_TARGET_BOUNDED_CONTEXT] {
            let bc = BoundedContext {
                name: name.to_string(),
                status: BoundedContextStatus::Active,
                created_at: test_now(),
                created_by: ContextCreator::SystemCreator,
                template: None,
            };
            db::insert_bounded_context(&pool, &bc).await.unwrap();
            let mapping = RoleAccessMapping {
                role: role.clone(),
                bounded_context: bc,
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            };
            db::insert_role_access_mapping(&pool, &mapping)
                .await
                .unwrap();
            source_mapping.get_or_insert(mapping);
        }

        let build = || {
            let (database_url, external_subject) = (database_url.clone(), external_subject.clone());
            async move {
                Skilj::builder(database_url)
                    .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
                    .bounded_context(SHUTDOWN_SOURCE_BOUNDED_CONTEXT)
                    .event_type::<ParcelSent>()
                    .bounded_context(SHUTDOWN_TARGET_BOUNDED_CONTEXT)
                    .event_type::<LabelPrinted>()
                    .command_type::<PrintLabel>()
                    .cross_context_route::<ParcelsToLabels>()
                    .cross_context_route_poll_interval(std::time::Duration::from_millis(50))
                    .reconciliation_role(external_subject)
                    .build()
                    .await
                    .unwrap()
                    .0
            }
        };
        let skilj = build().await;
        let event_type = db::get_event_type(&pool, SHUTDOWN_SOURCE_BOUNDED_CONTEXT, "ParcelSent")
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_direct_creation_token(
            source_mapping.as_ref().unwrap(),
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &token)
            .await
            .unwrap();
        let credential = format!("{}.{}", token.id, token.secret);
        let send = |router: axum::Router, parcel: &'static str| {
            let credential = credential.clone();
            async move {
                let request = Request::builder()
                    .method("POST")
                    .uri("/v1/events/direct")
                    .header("authorization", format!("Bearer {credential}"))
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"payload":{{"parcel":"{parcel}"}}}}"#)))
                    .unwrap();
                let response = router.oneshot(request).await.unwrap();
                assert_eq!(response.status(), StatusCode::CREATED);
            }
        };
        let labels = || {
            let pool = pool.clone();
            async move {
                db::list_events_for_bounded_context(&pool, SHUTDOWN_TARGET_BOUNDED_CONTEXT)
                    .await
                    .unwrap()
                    .len()
            }
        };
        let hold_target_lock = || {
            let pool = pool.clone();
            async move {
                let mut blocker = pool.begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SELECT next_value FROM \"bc_{SHUTDOWN_TARGET_BOUNDED_CONTEXT}\".sequence FOR UPDATE"
                )))
                .execute(&mut *blocker)
                .await
                .unwrap();
                blocker
            }
        };
        let settle = || tokio::time::sleep(std::time::Duration::from_millis(500));

        // 1. A tick in progress completes.
        let blocker = hold_target_lock().await;
        send(skilj.rest_router(), "p1").await;
        settle().await;
        let shutdown = tokio::spawn(skilj.shutdown(std::time::Duration::from_secs(20)));
        settle().await;
        assert!(!shutdown.is_finished(), "shutdown must wait for the route's tick");
        blocker.commit().await.unwrap();
        let report = shutdown.await.unwrap();
        assert_eq!(labels().await, 1, "the tick in progress must complete");
        let mut stopped = report.stopped.clone();
        stopped.sort();
        assert_eq!(
            stopped,
            [
                "async_projections",
                "cancel_deadlines",
                "cross_context_routes",
                "cross_instance",
                "deadline_retention",
                "fire_deadlines",
                "idempotency_key_retention",
                "schedule_deadlines",
                "snapshots",
                "system_event_scheduler",
            ],
            "{report:?}"
        );
        assert!(report.aborted.is_empty(), "{report:?}");
        assert!(report.pool_closed, "{report:?}");

        // 2. A tick still busy at the timeout is aborted.
        let skilj = build().await;
        let blocker = hold_target_lock().await;
        send(skilj.rest_router(), "p2").await;
        settle().await;
        let report = skilj.shutdown(std::time::Duration::from_secs(1)).await;
        assert_eq!(report.aborted, ["cross_context_routes"], "{report:?}");
        blocker.commit().await.unwrap();
        settle().await;
        assert_eq!(labels().await, 1, "the aborted tick must not have written");

        // 3. Dropping stops nothing: the route picks up p2.
        let skilj = build().await;
        let _router = skilj.rest_router();
        drop(skilj);
        let mut printed = 0;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            printed = labels().await;
            if printed == 2 {
                break;
            }
        }
        assert_eq!(printed, 2, "a dropped Skilj's route loop must keep running");
    });
}
