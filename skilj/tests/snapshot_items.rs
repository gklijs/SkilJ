//! A command's snapshot items (docs/architecture.md §202): `decide_with()`
//! gets a snapshot's state for each tag value its query names, brought up
//! to date by the framework, next to the events its other items select.
//! The fixture is a transfer between two accounts: each account's balance
//! comes from the `Balance` snapshot, which folds a transfer into both
//! accounts it carries (a debit for one, a credit for the other), and
//! whether the transfer id was used comes from a plain item.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj, Snapshot};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event, EventBroadcaster};
use skilj_core::plugin::{BoundedContextEvent, Decision};
use skilj_core::shared::{
    generate_token_id, CommandDecision, EventSpec, QueryItemMapping, TagMapping,
};

fn mapping(key: &str, field: &str) -> TagMapping {
    TagMapping {
        key: key.into(),
        field: field.into(),
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct DepositedPayload {
    account_id: String,
    amount: i64,
}

struct Deposited;

impl EventType for Deposited {
    type Payload = DepositedPayload;
    const NAME: &'static str = "Deposited";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![mapping("account", "account_id")]
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct TransferredPayload {
    from: String,
    to: String,
    amount: i64,
    transfer_id: String,
}

struct Transferred;

impl EventType for Transferred {
    type Payload = TransferredPayload;
    const NAME: &'static str = "Transferred";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![
            mapping("account", "from"),
            mapping("account", "to"),
            mapping("transfer", "transfer_id"),
        ]
    }
}

enum LedgerEvent {
    Deposited(DepositedPayload),
    Transferred(TransferredPayload),
}

impl BoundedContextEvent for LedgerEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "Deposited" => Some(serde_json::from_str(&event.payload).map(LedgerEvent::Deposited)),
            "Transferred" => {
                Some(serde_json::from_str(&event.payload).map(LedgerEvent::Transferred))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct BalanceState {
    balance: i64,
}

/// One account's balance. A transfer carries both accounts, and is folded
/// into each: `fold_for` tells which side `account` is on.
struct Balance;

impl Snapshot for Balance {
    type State = BalanceState;
    type Event = LedgerEvent;
    const NAME: &'static str = "Balance";
    const TAG_KEY: &'static str = "account";
    const VERSION: u64 = 1;
    fn fold_for(state: &mut Self::State, account: &str, event: &Self::Event) {
        match event {
            LedgerEvent::Deposited(d) => state.balance += d.amount,
            LedgerEvent::Transferred(t) if t.from == account => state.balance -= t.amount,
            LedgerEvent::Transferred(t) if t.to == account => state.balance += t.amount,
            LedgerEvent::Transferred(_) => {}
        }
    }
}

struct Deposit;

impl CommandType for Deposit {
    type Payload = DepositedPayload;
    type Event = LedgerEvent;
    const NAME: &'static str = "Deposit";
    fn tag_mappings() -> Vec<TagMapping> {
        Deposited::tag_mappings()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::nothing()]
    }
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "Deposited".into(),
                payload: serde_json::to_value(payload).unwrap(),
            }],
        }
    }
}

/// Needs the source account's balance and whether its transfer id was
/// used - and nothing else of either account's history.
struct Transfer;

impl CommandType for Transfer {
    type Payload = TransferredPayload;
    type Event = LedgerEvent;
    const NAME: &'static str = "Transfer";
    fn tag_mappings() -> Vec<TagMapping> {
        Transferred::tag_mappings()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![
            QueryItemMapping::snapshot::<Balance>("from"),
            QueryItemMapping::snapshot::<Balance>("to"),
            QueryItemMapping::types(&["Transferred"])
                .tagged(vec![mapping("transfer", "transfer_id")])
                .latest(),
        ]
    }
    fn decide_with(
        payload: &Self::Payload,
        decision: &Decision<'_, Self::Event>,
    ) -> CommandDecision {
        let reject = |kind: &str| CommandDecision::Rejected {
            reason: kind.into(),
            kind: kind.into(),
        };
        if !decision.events().is_empty() {
            return reject("transfer_id_used");
        }
        let (Some(from), Some(_to)) = (
            decision.snapshot::<Balance>(&payload.from),
            decision.snapshot::<Balance>(&payload.to),
        ) else {
            return reject("no_balance");
        };
        if from.balance < payload.amount {
            return reject("insufficient_funds");
        }
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "Transferred".into(),
                payload: serde_json::to_value(payload).unwrap(),
            }],
        }
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

fn now() -> chrono::DateTime<chrono::Utc> {
    use chrono::SubsecRound;
    chrono::Utc::now().trunc_subsecs(6)
}

struct Ledger {
    skilj: Skilj,
    pool: Pool,
    bc: String,
    cache: EventCache,
}

/// The binary's one test database, made once: `database_url` drops and
/// recreates it, so tests calling it each raced on `CREATE DATABASE`, and
/// the losers skipped.
static TEST_DB: tokio::sync::OnceCell<Option<(String, Pool)>> = tokio::sync::OnceCell::const_new();

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(|| async {
            let database_url = skilj_test_support::database_url("skilj_snapshot_items").await?;
            let pool = db::connect(&database_url).await.unwrap();
            db::migrate(&pool).await.unwrap();
            Some((database_url, pool))
        })
        .await
        .clone()
}

/// A fresh bounded context with the ledger's types, built by a `Skilj`
/// whose reconciliation role has admin access to it. `None` without a
/// test database.
async fn ledger() -> Option<Ledger> {
    let (database_url, pool) = test_db().await?;
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
    db::insert_role(&pool, &role).await.unwrap();
    let bc_name = format!("ledger_{}", generate_token_id());
    let bc = BoundedContext {
        name: bc_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(&pool, &bc).await.unwrap();
    db::insert_role_access_mapping(
        &pool,
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
        .bounded_context(bc_name.clone())
        .event_type::<Deposited>()
        .event_type::<Transferred>()
        .command_type::<Deposit>()
        .command_type::<Transfer>()
        .snapshot::<Balance>()
        .reconciliation_role(subject)
        .build()
        .await
        .unwrap();
    Some(Ledger {
        skilj,
        pool,
        bc: bc_name,
        cache: EventCache::new(1000),
    })
}

impl Ledger {
    async fn command_type(&self, name: &str) -> skilj_core::event_store::CommandType {
        db::get_command_type(&self.pool, &self.bc, name)
            .await
            .unwrap()
            .unwrap()
    }

    async fn submit(&self, name: &str, payload: serde_json::Value) -> SubmitCommandOutcome {
        db::decide_and_submit_command(
            &self.pool,
            &*self.skilj.command_dispatcher(),
            &*self.skilj.projection_dispatcher(),
            &*self.skilj.snapshot_dispatcher(),
            &EventBroadcaster::new(16),
            &self.cache,
            &self.command_type(name).await,
            &payload.to_string(),
            "client",
            None,
            None,
            None,
            now(),
            None,
        )
        .await
        .unwrap()
    }

    async fn deposit(&self, account: &str, amount: i64) {
        let outcome = self
            .submit(
                "Deposit",
                serde_json::json!({ "account_id": account, "amount": amount }),
            )
            .await;
        assert_eq!(kind(&outcome), "accepted");
    }

    async fn transfer(&self, from: &str, to: &str, amount: i64, id: &str) -> String {
        let outcome = self
            .submit("Transfer", transfer(from, to, amount, id))
            .await;
        kind(&outcome).to_string()
    }

    async fn catch_up(&self) {
        db::catch_up_snapshots(&self.pool, &self.bc, &*self.skilj.snapshot_dispatcher())
            .await
            .unwrap();
    }

    /// The stored row's balance and the version it reports.
    async fn stored(&self, account: &str) -> (i64, u64) {
        let (version, _, state, _) =
            db::get_snapshot_state(&self.pool, &self.bc, "Balance", "account", account)
                .await
                .unwrap()
                .unwrap();
        let state: BalanceState = serde_json::from_str(&state).unwrap();
        (state.balance, version)
    }
}

fn transfer(from: &str, to: &str, amount: i64, id: &str) -> serde_json::Value {
    serde_json::json!({ "from": from, "to": to, "amount": amount, "transfer_id": id })
}

fn kind(outcome: &SubmitCommandOutcome) -> &str {
    match outcome {
        SubmitCommandOutcome::Accepted { .. } => "accepted",
        SubmitCommandOutcome::Rejected { kind, .. } => kind,
        SubmitCommandOutcome::Deduplicated { .. } => "deduplicated",
    }
}

fn account() -> String {
    format!("account_{}", generate_token_id())
}

/// `decide_with()` gets both accounts' balances, folded from their whole
/// history while no row is stored, then from the stored rows plus the
/// events since. A transfer is folded into both accounts it carries, and
/// a used transfer id is found through the plain item.
#[test]
fn a_transfer_decides_on_both_balances_and_its_id() {
    runtime().block_on(async {
        let Some(l) = ledger().await else {
            return;
        };
        let (a, b) = (account(), account());
        l.deposit(&a, 100).await;
        assert_eq!(l.transfer(&a, &b, 60, "t1").await, "accepted");
        assert_eq!(l.transfer(&a, &b, 60, "t2").await, "insufficient_funds");
        assert_eq!(l.transfer(&b, &a, 10, "t1").await, "transfer_id_used");

        l.catch_up().await;
        assert_eq!(l.stored(&a).await, (40, 1));
        assert_eq!(l.stored(&b).await, (60, 1));

        // From the stored rows, plus a deposit folded on top.
        l.deposit(&a, 5).await;
        assert_eq!(l.transfer(&b, &a, 60, "t3").await, "accepted");
        assert_eq!(l.transfer(&a, &b, 106, "t4").await, "insufficient_funds");
        assert_eq!(l.transfer(&a, &b, 105, "t4").await, "accepted");
        l.catch_up().await;
        assert_eq!(l.stored(&a).await, (0, 1));
        assert_eq!(l.stored(&b).await, (105, 1));
        l.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// A transfer out of the same account commits between a transfer's read
/// and its lock. It carries the account's tag, so it conflicts: it is
/// folded into the balance the waiting transfer decided on, which is then
/// decided again and refused.
#[test]
fn an_event_committed_after_the_read_is_folded_in_before_deciding_again() {
    runtime().block_on(async {
        let Some(l) = ledger().await else {
            return;
        };
        let (a, b, c) = (account(), account(), account());
        l.deposit(&a, 50).await;
        l.catch_up().await;

        let transfer_type = l.command_type("Transfer").await;
        let payload = transfer(&a, &b, 40, "late").to_string();
        let resolved = db::resolve_command_submission(
            &l.pool,
            &*l.skilj.command_dispatcher(),
            &*l.skilj.snapshot_dispatcher(),
            &l.cache,
            &transfer_type,
            &payload,
        )
        .await
        .unwrap();
        assert!(matches!(
            resolved.decision,
            CommandDecision::Accepted { .. }
        ));
        assert_eq!(resolved.snapshot_states.len(), 2);

        assert_eq!(l.transfer(&a, &c, 30, "early").await, "accepted");

        let outcome = db::submit_command(
            &l.pool,
            &*l.skilj.command_dispatcher(),
            &*l.skilj.projection_dispatcher(),
            &EventBroadcaster::new(16),
            &l.cache,
            &transfer_type,
            &payload,
            "client",
            None,
            None,
            &resolved.bounded_context_events,
            &resolved.consistency_tags,
            &resolved.matching_events,
            resolved.decision,
            None,
            now(),
            None,
            &resolved.snapshot_states,
            None,
            Some(resolved.covered_through),
        )
        .await
        .unwrap();
        assert_eq!(kind(&outcome), "insufficient_funds");
        l.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// A row folded under the old rule - first value only, before §202, or by
/// an older instance during a rolling deploy - carries no every-value
/// stamp. A command treats it as absent and folds the account's whole
/// history; catch-up resets and refolds it once an event touches the
/// account.
#[test]
fn a_row_from_before_every_value_folding_is_never_trusted() {
    runtime().block_on(async {
        let Some(l) = ledger().await else {
            return;
        };
        let (a, b) = (account(), account());
        l.deposit(&a, 70).await;
        l.catch_up().await;
        let schema = format!("\"bc_{}\"", l.bc);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.snapshots SET snapshot_version = 1, \
             state = '{{\"balance\": 1000}}'::jsonb WHERE tag_value = $1"
        )))
        .bind(&a)
        .execute(&l.pool)
        .await
        .unwrap();
        assert_eq!(
            l.stored(&a).await,
            (1000, 0),
            "reported as no current version"
        );

        assert_eq!(l.transfer(&a, &b, 100, "t1").await, "insufficient_funds");
        assert_eq!(l.transfer(&a, &b, 70, "t2").await, "accepted");
        l.catch_up().await;
        assert_eq!(l.stored(&a).await, (0, 1));
        l.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}
