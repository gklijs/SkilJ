//! Real end-to-end proof of the drift audit's #8 fix (the in-memory
//! per-bounded-context event cache - see project memory
//! `skilj-drift-audit-2026-08-18`): a real `Skilj`, a small
//! `.event_cache_warm_up_count(...)` so eviction is cheap to force, and
//! a genuine `ProcessCommand` DCB conflict whose own conflicting event
//! has scrolled out of the cache's own window by the time the command
//! is triggered. If the DCB pre-check trusted the cache's own
//! (incomplete) view instead of correctly falling back to Postgres, this
//! conflict would be silently missed and the command wrongly accepted -
//! `skilj-core/tests/event_cache.rs` already proves `EventCache` itself
//! is correct in isolation; this proves the real REST call site
//! (`post_commands_trigger`) is actually wired to it and the fallback
//! path is genuinely exercised, not just the fast path. Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj/tests/event_creation_atomicity.rs` - see its own doc comment
//! for the details, not repeated a third time here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event, EventOrigin};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{
    generate_token_id, generate_token_secret, CommandDecision, EventSpec, Metadata, Tag, TagMapping,
};
use tower::ServiceExt;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OrderShippedPayload {
    order_id: String,
}

struct OrderShipped;

impl EventType for OrderShipped {
    type Payload = OrderShippedPayload;
    const NAME: &'static str = "OrderShipped";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping {
            key: "order".into(),
            field: "order_id".into(),
        }]
    }
}

enum OrderEvent {
    // `decide()` below only ever checks `matching_events.is_empty()` -
    // never destructures the payload - so this field is structurally
    // required (matches `BoundedContextEvent`'s own shape) but never
    // read.
    #[allow(dead_code)]
    OrderShipped(OrderShippedPayload),
}

impl BoundedContextEvent for OrderEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "OrderShipped" => {
                Some(serde_json::from_str(&event.payload).map(OrderEvent::OrderShipped))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct ShipOrderPayload {
    order_id: String,
}

struct ShipOrder;

impl CommandType for ShipOrder {
    type Payload = ShipOrderPayload;
    type Event = OrderEvent;
    const NAME: &'static str = "ShipOrder";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping {
            key: "order".into(),
            field: "order_id".into(),
        }]
    }
    /// "Can't ship the same order twice" - real DCB-guarded behaviour,
    /// the same shape `skilj-core/tests/submit_command.rs`'s own
    /// `ShipOrder` fixture uses.
    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        if matching_events.is_empty() {
            CommandDecision::Accepted {
                events: vec![EventSpec {
                    event_type: "OrderShipped".to_string(),
                    payload: serde_json::json!({ "order_id": payload.order_id }),
                }],
            }
        } else {
            CommandDecision::Rejected {
                reason: "already shipped".to_string(),
                kind: "already_shipped".to_string(),
            }
        }
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
            .expect("failed to build a tokio runtime for event_cache tests")
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
        let database_name = "skilj_event_cache_e2e_test";
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
fn a_dcb_conflict_outside_the_cache_window_is_still_caught_via_the_postgres_fallback() {
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

        let bc_name = unique_name("orders");
        let bc = BoundedContext {
            name: bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
        };
        db::insert_bounded_context(&pool, &bc).await.unwrap();

        let mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping)
            .await
            .unwrap();

        // Registers OrderShipped/ShipOrder for real, but a tiny warm-up
        // count - just 2 - so the cache's own window can only ever hold
        // the 2 most recent events, cheap to force eviction against.
        let (skilj, report) = Skilj::builder(database_url)
            .bounded_context(bc_name.clone())
            .event_type::<OrderShipped>()
            .command_type::<ShipOrder>()
            .reconciliation_role(external_subject)
            .event_cache_warm_up_count(2)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        // 3 pre-existing OrderShipped events, inserted directly - the
        // cache's own warm-up (capacity 2) only ever gets to hold the
        // last 2 of these (order-2, order-3), so order-1's own event -
        // the one this test's real conflict depends on - is evicted
        // before the cache is even warmed.
        let et = db::get_event_type(&pool, &bc_name, "OrderShipped")
            .await
            .unwrap()
            .unwrap();
        for order_id in ["order-1", "order-2", "order-3"] {
            let seq = db::next_sequence(&pool, &bc_name).await.unwrap();
            let event = Event {
                bounded_context: bc.clone(),
                event_type: et.clone(),
                payload: format!(r#"{{"order_id":"{order_id}"}}"#),
                metadata: Metadata {
                    r#type: "OrderShipped".to_string(),
                    version: 1,
                    client_id: "pre-existing".to_string(),
                    created_at: test_now(),
                },
                sequence: seq,
                tags: vec![Tag {
                    key: "order".to_string(),
                    value: Some(order_id.to_string()),
                }],
                encryption_keys: Vec::new(),
                origin: EventOrigin::DirectlyCreated,
            };
            db::insert_event(&pool, &event, None).await.unwrap();
        }

        // `.build()` above warmed the cache against an empty history -
        // these 3 events didn't exist yet. Nothing re-warms it
        // explicitly: the very first cache-served read below finds it
        // stale (via the same freshness check
        // `skilj-core/tests/event_cache.rs`'s own multi-instance test
        // already proves) and self-heals via the identical Postgres
        // catch-up, capacity-evicting order-1 exactly as a real warm-up
        // would have. Real end to end: no direct `EventCache` handle
        // this test could reach into private facade state for - only
        // the real REST trigger below.

        let command_type = db::get_command_type(&pool, &bc_name, "ShipOrder")
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_command_token(
            &mapping,
            &command_type,
            generate_token_id(),
            generate_token_secret(),
            test_now(),
        )
        .unwrap();
        db::insert_command_token(&pool, &token).await.unwrap();
        let credential = format!("{}.{}", token.id, token.secret);

        let router = skilj.rest_router();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"order_id":"order-1"}}"#))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        // The whole point: order-1 was already shipped, by an event the
        // cache's own window can't have covered (capacity 2, 3rd-oldest
        // of 3) - a correct DCB pre-check must still catch this via its
        // own Postgres fallback, not accept a duplicate shipment because
        // the fast path silently missed it.
        assert_eq!(json["accepted"], false);
        assert_eq!(json["rejectionKind"], "already_shipped");
    });
}
