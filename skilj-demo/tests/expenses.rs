//! A worked example of corrections and retractions (docs/architecture.md §235,
//! issue #133) on an amount rather than a unique value: an expense report whose
//! total is a projection. A retraction doesn't recompute a projection, so
//! `ReportTotal` remembers what each event contributed, to take it out again
//! when that event is retracted.
//!
//! Where the dossier goes: in the correction command's payload. Each event's
//! `causation_id` is its command's id, so the retraction leads to it, and a
//! payload field can be sensitive where a retraction's `reason` can't.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::retraction::{
    claim_query, corrected, retract, standing, Retractable, RetractionGround, RETRACTED_EVENT_TYPE,
};
use skilj::{CommandType, EventType, KeyService, Projection, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event, EventBroadcaster};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{
    generate_token_id, CommandDecision, EventSpec, QueryItemMapping, Tag, TagMapping,
};
use skilj_test_fixture::projection::GivenEvents;

const BOUNDED_CONTEXT: &str = "skilj_demo_expenses";

fn report() -> TagMapping {
    TagMapping::new("report", "report_id")
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct Expense {
    report_id: String,
    amount_cents: i64,
}

struct ExpenseRecorded;

impl EventType for ExpenseRecorded {
    type Payload = Expense;
    const NAME: &'static str = "ExpenseRecorded";
    const BOUNDED_CONTEXT: &'static str = BOUNDED_CONTEXT;
    fn tag_mappings() -> Vec<TagMapping> {
        vec![report()]
    }
}

struct Recorded(Expense);

impl BoundedContextEvent for Recorded {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        (event.event_type.name == "ExpenseRecorded")
            .then(|| serde_json::from_str(&event.payload).map(Recorded))
    }
}

fn recorded(expense: &Expense) -> EventSpec {
    EventSpec {
        event_type: "ExpenseRecorded".into(),
        payload: serde_json::to_value(expense).unwrap(),
    }
}

struct RecordExpense;

impl CommandType for RecordExpense {
    type Payload = Expense;
    type Event = Recorded;
    const NAME: &'static str = "RecordExpense";
    const BOUNDED_CONTEXT: &'static str = BOUNDED_CONTEXT;
    fn tag_mappings() -> Vec<TagMapping> {
        vec![report()]
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::nothing()]
    }
    fn decide(payload: &Expense, _: &[Recorded]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![recorded(payload)],
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Correction {
    report_id: String,
    position: i64,
    amount_cents: i64,
    /// The dossier this correction comes from. In the command, not the
    /// retraction: reached through the event's `causation_id`.
    case: String,
}

/// Whether `position` is an expense of this report nobody has retracted. The
/// store checks the position exists and isn't retracted already; that it
/// belongs to this report is the application's.
fn stands(events: &[Retractable<Recorded>], position: i64) -> bool {
    standing(events)
        .iter()
        .any(|expense| expense.position == position)
}

fn not_in_report() -> CommandDecision {
    CommandDecision::Rejected {
        reason: "no such expense on this report".into(),
        kind: "not_in_report".into(),
    }
}

/// A wrong amount: retracted, and the right one recorded, in one append.
struct CorrectExpense;

impl CommandType for CorrectExpense {
    type Payload = Correction;
    type Event = Retractable<Recorded>;
    const NAME: &'static str = "CorrectExpense";
    const BOUNDED_CONTEXT: &'static str = BOUNDED_CONTEXT;
    fn tag_mappings() -> Vec<TagMapping> {
        vec![report()]
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        claim_query(&["ExpenseRecorded"], report())
    }
    fn decide(payload: &Correction, events: &[Self::Event]) -> CommandDecision {
        if !stands(events, payload.position) {
            return not_in_report();
        }
        corrected(
            payload.position,
            RetractionGround::WrongDataSubmitted,
            "wrong amount",
            [recorded(&Expense {
                report_id: payload.report_id.clone(),
                amount_cents: payload.amount_cents,
            })],
        )
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct Withdrawal {
    report_id: String,
    position: i64,
}

/// An expense that should never have been recorded.
struct WithdrawExpense;

impl CommandType for WithdrawExpense {
    type Payload = Withdrawal;
    type Event = Retractable<Recorded>;
    const NAME: &'static str = "WithdrawExpense";
    const BOUNDED_CONTEXT: &'static str = BOUNDED_CONTEXT;
    fn tag_mappings() -> Vec<TagMapping> {
        vec![report()]
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        claim_query(&["ExpenseRecorded"], report())
    }
    fn decide(payload: &Withdrawal, events: &[Self::Event]) -> CommandDecision {
        if !stands(events, payload.position) {
            return not_in_report();
        }
        CommandDecision::Accepted {
            events: vec![retract(
                payload.position,
                RetractionGround::ProcessedWrongly,
                "booked twice",
            )],
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
struct Contribution {
    position: i64,
    amount_cents: i64,
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
struct ReportTotalState {
    total_cents: i64,
    /// What each standing event added, which is all taking one out needs.
    contributions: Vec<Contribution>,
}

/// The pattern for a projection under retractions: consume `EventRetracted`,
/// fold `Retractable` events, and keep enough in the state to undo one.
struct ReportTotal;

impl Projection for ReportTotal {
    type State = ReportTotalState;
    type Event = Retractable<Recorded>;
    const NAME: &'static str = "ReportTotal";
    const BOUNDED_CONTEXT: &'static str = BOUNDED_CONTEXT;
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["ExpenseRecorded", RETRACTED_EVENT_TYPE]
    }
    fn sync() -> bool {
        true
    }
    fn keys(event: &Self::Event) -> Vec<String> {
        // A retraction carries the tags of what it retracts, so the same key.
        let (Retractable::Event { tags, .. } | Retractable::Retraction { tags, .. }) = event else {
            return Vec::new();
        };
        tags.iter()
            .filter(|tag| tag.key == "report")
            .filter_map(|tag| tag.value.clone())
            .collect()
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            Retractable::Event {
                position, event, ..
            } => {
                state.total_cents += event.0.amount_cents;
                state.contributions.push(Contribution {
                    position: *position,
                    amount_cents: event.0.amount_cents,
                });
            }
            Retractable::Retraction { retraction, .. } => {
                state.contributions.retain(|contribution| {
                    if contribution.position == retraction.position {
                        state.total_cents -= contribution.amount_cents;
                        return false;
                    }
                    true
                });
            }
            _ => {}
        }
    }
}

fn report_tags(report_id: &str) -> Vec<Tag> {
    vec![Tag {
        key: "report".into(),
        value: Some(report_id.into()),
    }]
}

fn expense(position: i64, amount_cents: i64) -> Retractable<Recorded> {
    Retractable::event(
        position,
        report_tags("r"),
        Recorded(Expense {
            report_id: "r".into(),
            amount_cents,
        }),
    )
}

/// No database needed for the fold itself.
#[test]
fn the_total_takes_a_retracted_expense_out() {
    GivenEvents::<ReportTotal>::new()
        .events([
            expense(1, 100),
            expense(2, 200),
            Retractable::retraction(3, report_tags("r"), 1),
        ])
        .then_state(ReportTotalState {
            total_cents: 200,
            contributions: vec![Contribution {
                position: 2,
                amount_cents: 200,
            }],
        });
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

fn now() -> chrono::DateTime<chrono::Utc> {
    use chrono::SubsecRound;
    chrono::Utc::now().trunc_subsecs(6)
}

fn master_key() -> KeyService {
    KeyService::new(skilj_core::encryption::InMemoryKeyProvider::from_bytes(
        [6; 32],
    ))
}

struct Expenses {
    skilj: Skilj,
    pool: Pool,
    cache: EventCache,
}

impl Expenses {
    async fn submit(&self, name: &str, payload: serde_json::Value) -> String {
        let command_type = db::get_command_type(&self.pool, BOUNDED_CONTEXT, name)
            .await
            .unwrap()
            .unwrap();
        let outcome = db::decide_and_submit_command(
            &self.pool,
            &*self.skilj.command_dispatcher(),
            &*self.skilj.projection_dispatcher(),
            &*self.skilj.snapshot_dispatcher(),
            &EventBroadcaster::new(16),
            &self.cache,
            &command_type,
            &payload.to_string(),
            "client",
            None,
            None,
            Some(&master_key()),
            now(),
            None,
            None,
        )
        .await
        .unwrap();
        match outcome {
            SubmitCommandOutcome::Accepted { .. } => "accepted".into(),
            SubmitCommandOutcome::Rejected { kind, .. } => kind,
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn record(&self, report_id: &str, amount_cents: i64) -> i64 {
        let outcome = self
            .submit(
                "RecordExpense",
                serde_json::json!({ "report_id": report_id, "amount_cents": amount_cents }),
            )
            .await;
        assert_eq!(outcome, "accepted");
        self.events().await.last().unwrap().sequence
    }

    async fn events(&self) -> Vec<Event> {
        db::list_events_for_bounded_context(&self.pool, BOUNDED_CONTEXT)
            .await
            .unwrap()
    }

    async fn total(&self, report_id: &str) -> i64 {
        let state = db::get_projection_state(&self.pool, BOUNDED_CONTEXT, "ReportTotal", report_id)
            .await
            .unwrap()
            .expect("a stored row");
        serde_json::from_str::<ReportTotalState>(&state)
            .unwrap()
            .total_cents
    }
}

async fn expenses() -> Option<Expenses> {
    let database_url = skilj_test_support::database_url("skilj_demo_expenses_test").await?;
    let pool = db::connect(&database_url).await.unwrap();
    db::migrate(&pool).await.unwrap();
    let bounded_context = BoundedContext {
        name: BOUNDED_CONTEXT.to_string(),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(&pool, &bounded_context)
        .await
        .unwrap();
    let role = Role {
        id: generate_token_id(),
        external_subject: format!("subject_{}", generate_token_id()),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    db::insert_role(&pool, &role).await.unwrap();
    db::insert_role_access_mapping(
        &pool,
        &RoleAccessMapping {
            role: role.clone(),
            bounded_context,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            capabilities: Default::default(),
            delegable: Default::default(),
            status: RoleStatus::Active,
            created_at: now(),
            revoked_at: None,
        },
    )
    .await
    .unwrap();
    let (skilj, _) = Skilj::builder(database_url)
        .key_service(master_key())
        .reconciliation_role(role.external_subject)
        .bounded_context(BOUNDED_CONTEXT)
        .retractions()
        .event_type::<ExpenseRecorded>()
        .command_type::<RecordExpense>()
        .command_type::<CorrectExpense>()
        .command_type::<WithdrawExpense>()
        .projection::<ReportTotal>()
        .build()
        .await
        .unwrap();
    Some(Expenses {
        skilj,
        pool,
        cache: EventCache::new(1000),
    })
}

#[test]
fn a_report_total_follows_corrections_and_retractions() {
    runtime().block_on(async {
        let Some(expenses) = expenses().await else {
            return;
        };
        let lunch = expenses.record("q3", 100).await;
        let train = expenses.record("q3", 200).await;
        assert_eq!(expenses.total("q3").await, 300);

        let correction = serde_json::json!({
            "report_id": "q3",
            "position": train,
            "amount_cents": 250,
            "case": "dossier-17",
        });
        assert_eq!(
            expenses.submit("CorrectExpense", correction.clone()).await,
            "accepted"
        );
        assert_eq!(expenses.total("q3").await, 350);

        // The dossier is one step away: the retraction names its command.
        let events = expenses.events().await;
        let retraction = events
            .iter()
            .find(|event| event.event_type.name == RETRACTED_EVENT_TYPE)
            .unwrap();
        let command_id = retraction.metadata.causation_id.as_deref().unwrap();
        let command = db::get_command_by_external_id(&expenses.pool, BOUNDED_CONTEXT, command_id)
            .await
            .unwrap()
            .expect("the correction command");
        let stored_payload: serde_json::Value = serde_json::from_str(&command.payload).unwrap();
        assert_eq!(stored_payload["case"], "dossier-17");

        // Corrected already, so it no longer stands.
        assert_eq!(
            expenses.submit("CorrectExpense", correction).await,
            "not_in_report"
        );

        let withdrawal = serde_json::json!({ "report_id": "q3", "position": lunch });
        assert_eq!(
            expenses.submit("WithdrawExpense", withdrawal.clone()).await,
            "accepted"
        );
        assert_eq!(expenses.total("q3").await, 250);
        assert_eq!(
            expenses.submit("WithdrawExpense", withdrawal).await,
            "not_in_report"
        );

        // Another report's expense isn't this report's to withdraw.
        let other = expenses.record("q4", 500).await;
        let across = serde_json::json!({ "report_id": "q3", "position": other });
        assert_eq!(
            expenses.submit("WithdrawExpense", across).await,
            "not_in_report"
        );
        assert_eq!(expenses.total("q4").await, 500);

        expenses
            .skilj
            .shutdown(std::time::Duration::from_secs(10))
            .await;
    });
}
