//! End-to-end tests for `POST /v1/events/external`'s own `dedupe` field
//! ([docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event), specs/skilj.allium's own `rule
//! CreateExternalEvent`) - a real HTTP request, through
//! `Skilj::rest_router()`, into `db::create_and_insert_external_event`,
//! and back out through the real response body. `skilj-core/tests/external_event_dedup.rs`
//! already covers the mechanism itself against `db::create_and_insert_external_event`
//! directly; this file is the wire-level counterpart, proving the
//! `dedupe: { partitionKey, sequence }` JSON shape and the
//! `sequence`/`redelivered` response fields actually work over the real
//! REST surface, the same "layer in isolation, then the real wire" split
//! `skilj/tests/command_trigger.rs` already uses for `CommandTrigger`.
//! Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as that
//! file - see its own doc comment for the details, not repeated a third
//! time here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{EventType, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};
use tower::ServiceExt;

// --- fixtures ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OrderPlacedPayload {
    order_id: String,
}

struct OrderPlaced;

impl EventType for OrderPlaced {
    type Payload = OrderPlacedPayload;
    const NAME: &'static str = "OrderPlaced";
    fn external_creation_allowed() -> bool {
        true
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
            .expect("failed to build a tokio runtime for external_event_dedup tests")
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
    let database_url =
        skilj_test_support::database_url("skilj_external_event_dedup_rest_test").await?;

    let pool = connect_and_migrate(&database_url, "the test database").await?;
    Some(TestDb { database_url, pool })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn setup() -> (Skilj, String, Pool, String) {
    let (database_url, pool) = test_db()
        .await
        .expect("test_db() must be Some - caller already checked");

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
        template: None,
    };
    db::insert_bounded_context(&pool, &bc).await.unwrap();

    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
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

    let (skilj, report) = Skilj::builder(database_url)
        .bounded_context(bc_name.clone())
        .event_type::<OrderPlaced>()
        .reconciliation_role(external_subject)
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let event_type = db::get_event_type(&pool, &bc_name, "OrderPlaced")
        .await
        .unwrap()
        .unwrap();
    let token = access_control::create_external_event_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        test_now(),
    )
    .unwrap();
    db::insert_external_event_token(&pool, &token)
        .await
        .unwrap();

    let credential = format!("{}.{}", token.id, token.secret);
    (skilj, credential, pool, bc_name)
}

async fn submit(
    router: &axum::Router,
    credential: &str,
    order_id: &str,
    dedupe: Option<(&str, i64)>,
) -> (StatusCode, serde_json::Value) {
    let body = match dedupe {
        Some((partition_key, sequence)) => serde_json::json!({
            "payload": { "order_id": order_id },
            "sourceContent": "kafka",
            "dedupe": { "partitionKey": partition_key, "sequence": sequence },
        }),
        None => serde_json::json!({
            "payload": { "order_id": order_id },
            "sourceContent": "kafka",
        }),
    };
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events/external")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

/// [docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event): omitting `dedupe` entirely creates an event
/// every time, unchanged from before this feature existed -
/// `response.sequence` present, `response.redelivered` false.
#[test]
fn omitting_dedupe_creates_an_event_and_reports_no_redelivery() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, ..) = setup().await;
        let router = skilj.rest_router();

        let (status, body) = submit(&router, &credential, "A", None).await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(body["sequence"].is_i64());
        assert_eq!(body["redelivered"], false);
    });
}

/// The central case, over the real wire: a redelivered message (the
/// exact same `dedupe.partitionKey`/`dedupe.sequence` pair as an earlier,
/// already-created submission) still returns 201 - it's accepted, not an
/// error - but `sequence` is `null` and `redelivered` is `true`, and no
/// second event exists.
#[test]
fn a_redelivered_message_is_accepted_but_creates_nothing() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name) = setup().await;
        let router = skilj.rest_router();

        let (first_status, first_body) =
            submit(&router, &credential, "A", Some(("orders-topic:0", 10))).await;
        assert_eq!(first_status, StatusCode::CREATED);
        assert!(first_body["sequence"].is_i64());
        assert_eq!(first_body["redelivered"], false);

        let (redelivered_status, redelivered_body) =
            submit(&router, &credential, "A", Some(("orders-topic:0", 10))).await;
        assert_eq!(
            redelivered_status,
            StatusCode::CREATED,
            "a redelivery is accepted, not an error"
        );
        assert!(redelivered_body["sequence"].is_null());
        assert_eq!(redelivered_body["redelivered"], true);

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "the redelivery must not have created a second event"
        );
    });
}

/// `POST /v1/events/external/batch` with `events`, answered as JSON.
async fn submit_batch(
    router: &axum::Router,
    credential: &str,
    events: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events/external/batch")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "events": events }).to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// docs/architecture.md §227: a batch creates its events in the order
/// given, each answered as a single request would be. A message repeated
/// within the batch - by its cursor or by its key - is a redelivery, as
/// it would be sent twice; a payload off the schema is refused on its own
/// with a single request's status, and the events after it are created.
#[test]
fn an_external_event_batch_answers_each_event_in_order() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name) = setup().await;
        let router = skilj.rest_router();

        let event = |order: &str| {
            serde_json::json!({ "payload": { "order_id": order }, "sourceContent": "kafka" })
        };
        let with_cursor = |order: &str, sequence: i64| {
            serde_json::json!({
                "payload": { "order_id": order },
                "sourceContent": "kafka",
                "dedupe": { "partitionKey": "orders-topic:0", "sequence": sequence },
            })
        };
        let with_key = |order: &str, key: &str| {
            serde_json::json!({
                "payload": { "order_id": order },
                "sourceContent": "nats",
                "idempotencyKey": key,
            })
        };
        let (status, json) = submit_batch(
            &router,
            &credential,
            serde_json::json!([
                with_cursor("A", 1),
                with_cursor("A", 1),
                serde_json::json!({ "payload": { "order_id": 7 }, "sourceContent": "kafka" }),
                with_key("B", "stream:5"),
                with_key("B", "stream:5"),
                event("C"),
            ]),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{json}");
        let outcomes = json["outcomes"].as_array().unwrap();
        assert_eq!(outcomes.len(), 6, "{json}");
        assert_eq!(outcomes[0]["redelivered"], false);
        assert_eq!(outcomes[1]["redelivered"], true);
        assert!(outcomes[1]["sequence"].is_null());
        assert_eq!(outcomes[2]["error"]["status"], 400, "{json}");
        assert_eq!(outcomes[3]["redelivered"], false);
        assert_eq!(outcomes[4]["redelivered"], true);
        assert_eq!(outcomes[4]["sequence"], outcomes[3]["sequence"]);
        assert_eq!(outcomes[5]["redelivered"], false);

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        let orders: Vec<String> = events
            .iter()
            .map(|e| {
                serde_json::from_str::<serde_json::Value>(&e.payload).unwrap()["order_id"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(orders, vec!["A", "B", "C"]);
        // Gapless: the refused event took no sequence number.
        let sequences: Vec<i64> = events.iter().map(|e| e.sequence).collect();
        assert_eq!(sequences, vec![sequences[0], sequences[0] + 1, sequences[0] + 2]);
        assert_eq!(outcomes[0]["sequence"], sequences[0]);
        assert_eq!(outcomes[5]["sequence"], sequences[2]);

        // The cursor recorded by the batch dedupes a later single request.
        let (status, body) = submit(&router, &credential, "A", Some(("orders-topic:0", 1))).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["redelivered"], true);
    });
}

/// Over `max_items_per_batch` (100 by default) the batch is refused whole;
/// an empty one is answered with no outcomes.
#[test]
fn an_external_event_batch_over_the_limit_is_refused_whole() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name) = setup().await;
        let router = skilj.rest_router();

        let events: Vec<_> = (0..101)
            .map(|i| {
                serde_json::json!({ "payload": { "order_id": format!("o-{i}") }, "sourceContent": "kafka" })
            })
            .collect();
        let (status, json) = submit_batch(&router, &credential, serde_json::json!(events)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["code"], "batch_too_large");

        let (status, json) = submit_batch(&router, &credential, serde_json::json!([])).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["outcomes"], serde_json::json!([]));

        assert!(db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap()
            .is_empty());
    });
}

/// docs/architecture.md §227: with `stopAtFirstFailure`, nothing after a
/// failed event is recorded, so the partition's watermark stays below the
/// events answered `not_processed`, and sending them again creates them.
#[test]
fn an_external_event_batch_can_stop_at_its_first_failure() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name) = setup().await;
        let router = skilj.rest_router();
        let with_cursor = |order: serde_json::Value, sequence: i64| {
            serde_json::json!({
                "payload": { "order_id": order },
                "sourceContent": "kafka",
                "dedupe": { "partitionKey": "orders-topic:0", "sequence": sequence },
            })
        };

        let request = |events: serde_json::Value| {
            Request::builder()
                .method("POST")
                .uri("/v1/events/external/batch")
                .header("authorization", format!("Bearer {credential}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "stopAtFirstFailure": true, "events": events }).to_string(),
                ))
                .unwrap()
        };
        let response = router
            .clone()
            .oneshot(request(serde_json::json!([
                with_cursor(serde_json::json!("A"), 1),
                with_cursor(serde_json::json!(7), 2),
                with_cursor(serde_json::json!("C"), 3),
            ])))
            .await
            .unwrap();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let outcomes = json["outcomes"].as_array().unwrap();
        assert_eq!(outcomes[0]["redelivered"], false, "{json}");
        assert_eq!(outcomes[1]["error"]["status"], 400, "{json}");
        assert_eq!(outcomes[2]["error"]["code"], "not_processed", "{json}");

        // C again, on its own: below the watermark it would be dropped.
        let (status, body) = submit(&router, &credential, "C", Some(("orders-topic:0", 3))).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["redelivered"], false, "{body}");
        assert_eq!(
            db::list_events_for_bounded_context(&pool, &bc_name)
                .await
                .unwrap()
                .len(),
            2
        );
    });
}

/// docs/architecture.md §227: the bridges' batch client against the real
/// route - a redelivery is done, the first refused event stops the count,
/// and the partition's watermark stays below the events after it.
#[test]
fn the_bridges_batch_client_counts_what_the_external_route_got_done() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name) = setup().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = skilj.rest_router();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mapping = skilj_bridge::InboundMapping {
            credential,
            action: skilj_bridge::InboundAction::Record {
                event_type: "OrderPlaced".to_string(),
            },
        };
        let with_cursor = |order: serde_json::Value, sequence: i64| {
            serde_json::json!({
                "payload": { "order_id": order },
                "sourceContent": "kafka",
                "dedupe": { "partitionKey": "orders-topic:0", "sequence": sequence },
            })
        };
        let bodies = [
            with_cursor(serde_json::json!("A"), 1),
            with_cursor(serde_json::json!("A"), 1),
            with_cursor(serde_json::json!(7), 2),
            with_cursor(serde_json::json!("C"), 3),
        ];
        let items: Vec<_> = bodies
            .iter()
            .map(|body| skilj_bridge::InboundItem {
                body,
                idempotency_key: None,
            })
            .collect();
        let batcher = skilj_bridge::InboundBatcher::new(skilj_bridge::InboundBatching::default());
        let done = skilj_bridge::post_inbound_batch(
            &reqwest::Client::new(),
            &url,
            &mapping,
            &batcher,
            &items,
        )
        .await;
        assert_eq!(done, 2);
        assert_eq!(
            db::list_events_for_bounded_context(&pool, &bc_name)
                .await
                .unwrap()
                .len(),
            1
        );
    });
}
