//! `skilj::dev` (docs/architecture.md §220): what the template's and
//! `skilj-demo`'s servers seed a development database with - bounded
//! contexts, an admin role reconciliation registers types under, a
//! command token REST accepts, and an event read token that starts where
//! it was asked to.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj};
use skilj_core::event_store::Event;
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, CommandDecision, EventSpec};
use tower::ServiceExt;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct DepositedPayload {
    amount: i64,
}

struct Deposited;

impl EventType for Deposited {
    type Payload = DepositedPayload;
    const NAME: &'static str = "Deposited";
    fn event_read_allowed() -> bool {
        true
    }
}

enum WalletEvent {
    #[allow(
        dead_code,
        reason = "the event enum's shape needs this variant; this test never reads its payload"
    )]
    Deposited(DepositedPayload),
}

impl BoundedContextEvent for WalletEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "Deposited" => Some(serde_json::from_str(&event.payload).map(WalletEvent::Deposited)),
            _ => None,
        }
    }
}

struct Deposit;

impl CommandType for Deposit {
    type Payload = DepositedPayload;
    type Event = WalletEvent;
    const NAME: &'static str = "Deposit";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "Deposited".to_string(),
                payload: serde_json::json!({ "amount": payload.amount }),
            }],
        }
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

fn is_row_not_found(error: &skilj_core::Error) -> bool {
    matches!(error, skilj_core::Error::Database(sqlx::Error::RowNotFound))
}

#[test]
fn seeded_admin_registers_types_and_mints_a_working_command_token() {
    runtime().block_on(async {
        let Some(database_url) = skilj_test_support::database_url("skilj_dev_seed_test").await
        else {
            return;
        };
        let bc_name = format!("wallet_{}", generate_token_id());

        // Seeding twice against the same database is what a restarted
        // server does: the bounded context is reused, the role is new.
        let first = skilj::dev::seed_admin(&database_url, "first", &[&bc_name])
            .await
            .unwrap();
        let admin = skilj::dev::seed_admin(&database_url, "second", &[&bc_name])
            .await
            .unwrap();
        assert_ne!(first.external_subject(), admin.external_subject());
        first.close().await;

        let (skilj, report) = Skilj::builder(database_url)
            .pool_options(skilj::PgPoolOptions::new().max_connections(4))
            .bounded_context(bc_name.clone())
            .event_type::<Deposited>()
            .command_type::<Deposit>()
            .reconciliation_role(admin.external_subject())
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let token = admin
            .create_command_token(&bc_name, "Deposit")
            .await
            .unwrap();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({ "payload": { "amount": 7 } }).to_string(),
            ))
            .unwrap();
        let response = skilj.rest_router().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["accepted"], serde_json::json!(true), "{body}");

        let unregistered = admin.create_command_token(&bc_name, "Withdraw").await;
        assert!(is_row_not_found(&unregistered.unwrap_err()));
        let not_seeded = admin.create_command_token("elsewhere", "Deposit").await;
        assert!(is_row_not_found(&not_seeded.unwrap_err()));

        admin.close().await;
        skilj.shutdown(std::time::Duration::from_secs(5)).await;
    });
}

async fn deposit(router: &axum::Router, token: &str, amount: i64) {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/commands/trigger")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "payload": { "amount": amount } }).to_string(),
        ))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// The amounts of the events the token's first consume serves.
async fn first_consume(router: &axum::Router, token: &str) -> Vec<i64> {
    let request = Request::builder()
        .method("GET")
        .uri("/v1/events/consume?mode=auto")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    body["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["payload"]["amount"].as_i64().unwrap())
        .collect()
}

#[test]
fn event_read_tokens_start_at_the_beginning_or_where_they_were_minted() {
    runtime().block_on(async {
        // Its own database: `database_url` drops and recreates the one it
        // names, so sharing a name with the test above races it.
        let Some(database_url) =
            skilj_test_support::database_url("skilj_dev_seed_read_tokens_test").await
        else {
            return;
        };
        let bc_name = format!("wallet_{}", generate_token_id());
        let admin = skilj::dev::seed_admin(&database_url, "reader", &[&bc_name])
            .await
            .unwrap();
        let (skilj, _report) = Skilj::builder(database_url)
            .pool_options(skilj::PgPoolOptions::new().max_connections(4))
            .bounded_context(bc_name.clone())
            .event_type::<Deposited>()
            .command_type::<Deposit>()
            .reconciliation_role(admin.external_subject())
            .build()
            .await
            .unwrap();
        let router = skilj.rest_router();
        let command = admin
            .create_command_token(&bc_name, "Deposit")
            .await
            .unwrap();

        // Minted before anything was committed, Now starts at the start.
        let now_on_empty = admin
            .create_event_read_token(&bc_name, "Deposited", skilj::dev::ReadFrom::Now)
            .await
            .unwrap();
        deposit(&router, &command, 1).await;
        let beginning = admin
            .create_event_read_token(&bc_name, "Deposited", skilj::dev::ReadFrom::Beginning)
            .await
            .unwrap();
        // Pinned here, after event 1 - not at its first read, which comes
        // only after event 2 is committed.
        let now = admin
            .create_event_read_token(&bc_name, "Deposited", skilj::dev::ReadFrom::Now)
            .await
            .unwrap();
        deposit(&router, &command, 2).await;

        assert_eq!(first_consume(&router, &now).await, vec![2]);
        assert_eq!(first_consume(&router, &beginning).await, vec![1, 2]);
        assert_eq!(first_consume(&router, &now_on_empty).await, vec![1, 2]);

        let unregistered = admin
            .create_event_read_token(&bc_name, "Withdrawn", skilj::dev::ReadFrom::Now)
            .await;
        assert!(is_row_not_found(&unregistered.unwrap_err()));
        let not_seeded = admin
            .create_event_read_token("elsewhere", "Deposited", skilj::dev::ReadFrom::Beginning)
            .await;
        assert!(is_row_not_found(&not_seeded.unwrap_err()));

        admin.close().await;
        skilj.shutdown(std::time::Duration::from_secs(5)).await;
    });
}
