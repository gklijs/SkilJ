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
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
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
    _embedded: Option<postgresql_embedded::PostgreSQL>,
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
        let database_name = "skilj_cross_context_route_e2e_test";
        if let Err(e) = server.create_database(database_name).await {
            eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
            return None;
        }
        let url = server.settings().url(database_name);
        let pool = connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            pool,
            _embedded: Some(server),
        });
    };

    let pool = connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb {
        database_url,
        pool,
        _embedded: None,
    })
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
