//! `skilj::dev` (docs/architecture.md §220): what the template's and
//! `skilj-demo`'s servers seed a development database with - bounded
//! contexts, an admin role reconciliation registers types under, and a
//! command token REST accepts.

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
}

enum WalletEvent {
    #[allow(dead_code)]
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
