//! Retractions and corrections (docs/architecture.md §235, issue #133),
//! against Postgres, on the case that makes them hard: a unique email address
//! that is a blind tag, so `decide()` can't compare values and the old value
//! has to be released by tags alone.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::retraction::{
    claim_query, corrected, correction_query, retract, Retractable, RetractionGround,
    RetractionPayload, ValueClaims, ValueEffect, RETRACTED_EVENT_TYPE, RETRACTS_TAG_KEY,
};
use skilj::{
    normalize, BlindErasure, BlindIndexKey, BlindTag, CommandType, EventType, KeyService,
    Projection, Skilj, SkiljBuilder, Snapshot,
};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::command_batcher::{CommandBatchItem, CommandBatcher};
use skilj_core::db::{self, DryRunDecision, Pool, SubmitCommandOutcome};
use skilj_core::error::SkiljRejection;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event, EventBroadcaster};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{
    generate_token_id, CommandDecision, EventSpec, QueryItemMapping, SensitiveField, TagMapping,
};

struct Email;

impl BlindTag for Email {
    const TAG_KEY: &'static str = "email";
    const ERASURE: BlindErasure = BlindErasure::with_subject("customer");
    fn normalize(value: &str) -> String {
        normalize::email(value)
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct EmailPayload {
    customer_id: String,
    email: String,
}

fn customer() -> TagMapping {
    TagMapping::new("customer", "customer_id")
}

fn email() -> TagMapping {
    TagMapping::blind::<Email>("email")
}

fn encrypted_email() -> Vec<SensitiveField> {
    vec![SensitiveField::new("email", "customer", "customer_id")]
}

struct EmailClaimed;

impl EventType for EmailClaimed {
    type Payload = EmailPayload;
    const NAME: &'static str = "EmailClaimed";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        encrypted_email()
    }
}

struct EmailReleased;

impl EventType for EmailReleased {
    type Payload = EmailPayload;
    const NAME: &'static str = "EmailReleased";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        encrypted_email()
    }
}

/// Which of the two it is; a decision goes by tags, not by the address.
enum CustomerEvent {
    Claimed,
    Released,
}

impl BoundedContextEvent for CustomerEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "EmailClaimed" => Some(Ok(CustomerEvent::Claimed)),
            "EmailReleased" => Some(Ok(CustomerEvent::Released)),
            _ => None,
        }
    }
}

fn claims(events: &[Retractable<CustomerEvent>]) -> ValueClaims<'_, CustomerEvent> {
    ValueClaims::fold(events, "email", |event| match event {
        CustomerEvent::Claimed => ValueEffect::Claims,
        CustomerEvent::Released => ValueEffect::Releases,
    })
}

fn taken() -> CommandDecision {
    CommandDecision::Rejected {
        reason: "this email address is taken".into(),
        kind: "email_taken".into(),
    }
}

fn claim(payload: &EmailPayload) -> EventSpec {
    EventSpec {
        event_type: "EmailClaimed".into(),
        payload: serde_json::to_value(payload).unwrap(),
    }
}

const CLAIMS_AND_RELEASES: [&str; 2] = ["EmailClaimed", "EmailReleased"];

struct RegisterCustomer;

impl CommandType for RegisterCustomer {
    type Payload = EmailPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "RegisterCustomer";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        encrypted_email()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        claim_query(&CLAIMS_AND_RELEASES, email())
    }
    fn decide(payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        if !claims(events).held().is_empty() {
            return taken();
        }
        CommandDecision::Accepted {
            events: vec![claim(payload)],
        }
    }
}

struct CorrectEmail;

impl CommandType for CorrectEmail {
    type Payload = EmailPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "CorrectEmail";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        encrypted_email()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        correction_query(&CLAIMS_AND_RELEASES, customer(), email())
    }
    fn decide(payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        let claims = claims(events);
        if claims
            .held_by_another("customer", &payload.customer_id)
            .is_some()
        {
            return taken();
        }
        let Some(current) = claims.held_by("customer", &payload.customer_id) else {
            return CommandDecision::Rejected {
                reason: "this customer holds no address to correct".into(),
                kind: "nothing_to_correct".into(),
            };
        };
        corrected(
            current.position,
            RetractionGround::WrongDataSubmitted,
            "typo",
            [claim(payload)],
        )
    }
}

/// An ordinary release, not a correction: the address was right, and now
/// isn't the customer's any more.
struct ReleaseEmail;

impl CommandType for ReleaseEmail {
    type Payload = EmailPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "ReleaseEmail";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        encrypted_email()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        claim_query(&CLAIMS_AND_RELEASES, email())
    }
    fn decide(payload: &Self::Payload, _events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "EmailReleased".into(),
                payload: serde_json::to_value(payload).unwrap(),
            }],
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct RetractPayload {
    position: i64,
    #[serde(default)]
    customer_id: Option<String>,
}

/// Retracts whatever position it is given, the store being what checks it.
struct RetractAny;

impl CommandType for RetractAny {
    type Payload = RetractPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "RetractAny";
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::nothing()]
    }
    fn decide(payload: &Self::Payload, _events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![retract(
                payload.position,
                RetractionGround::ProcessedWrongly,
                "duplicate import",
            )],
        }
    }
}

/// [`RetractAny`] for a caller scoped to one customer.
struct RetractOwn;

impl CommandType for RetractOwn {
    type Payload = RetractPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "RetractOwn";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer()]
    }
    fn owner_tag_key() -> Option<&'static str> {
        Some("customer")
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::nothing()]
    }
    fn decide(payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        RetractAny::decide(payload, events)
    }
}

/// Knows nothing about retractions: the store keeps them out of its fold.
struct ClaimCount;

impl Snapshot for ClaimCount {
    type State = u32;
    type Event = CustomerEvent;
    const NAME: &'static str = "ClaimCount";
    const TAG_KEY: &'static str = "customer";
    const VERSION: u64 = 1;
    fn fold(state: &mut Self::State, event: &Self::Event) {
        if matches!(event, CustomerEvent::Claimed) {
            *state += 1;
        }
    }
}

/// [`ClaimCount`], partitioned, which catches up through its own path.
struct PartitionedClaimCount;

impl Snapshot for PartitionedClaimCount {
    type State = u32;
    type Event = CustomerEvent;
    const NAME: &'static str = "PartitionedClaimCount";
    const TAG_KEY: &'static str = "customer";
    const VERSION: u64 = 1;
    const PARTITION_COUNT: u32 = 3;
    fn fold(state: &mut Self::State, event: &Self::Event) {
        ClaimCount::fold(state, event);
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct CountPayload {
    customer_id: String,
}

/// Rejects with the count it was handed, for a test to read.
struct ReportClaimCount;

impl CommandType for ReportClaimCount {
    type Payload = CountPayload;
    type Event = CustomerEvent;
    const NAME: &'static str = "ReportClaimCount";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer()]
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::snapshot::<ClaimCount>("customer_id")]
    }
    fn decide_with(
        payload: &Self::Payload,
        decision: &skilj_core::plugin::Decision<'_, Self::Event>,
    ) -> CommandDecision {
        let count = decision
            .snapshot::<ClaimCount>(&payload.customer_id)
            .unwrap_or_default();
        CommandDecision::Rejected {
            reason: "reporting".into(),
            kind: format!("count_{count}"),
        }
    }
}

/// Decides from a stored row by the old API, which a retraction can't reach.
struct OldApiSnapshotCommand;

impl CommandType for OldApiSnapshotCommand {
    type Payload = CountPayload;
    type Event = CustomerEvent;
    const NAME: &'static str = "OldApiSnapshotCommand";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer()]
    }
    fn snapshot() -> Option<&'static str> {
        Some(ClaimCount::NAME)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct CurrentClaim {
    position: Option<i64>,
}

/// Takes a retracted claim out itself: a fold can't be undone from outside.
struct CurrentClaims;

impl Projection for CurrentClaims {
    type State = CurrentClaim;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "CurrentClaims";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["EmailClaimed", RETRACTED_EVENT_TYPE]
    }
    fn keys(event: &Self::Event) -> Vec<String> {
        let (Retractable::Event { tags, .. } | Retractable::Retraction { tags, .. }) = event else {
            return Vec::new();
        };
        tags.iter()
            .filter(|tag| tag.key == "customer")
            .filter_map(|tag| tag.value.clone())
            .collect()
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            Retractable::Event { position, .. } => state.position = Some(*position),
            Retractable::Retraction { retraction, .. }
                if state.position == Some(retraction.position) =>
            {
                state.position = None;
            }
            _ => {}
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

fn master_key() -> KeyService {
    KeyService::new(skilj_core::encryption::InMemoryKeyProvider::from_bytes(
        [7; 32],
    ))
}

static TEST_DB: tokio::sync::OnceCell<Option<(String, Pool)>> = tokio::sync::OnceCell::const_new();

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(|| async {
            let database_url = skilj_test_support::database_url("skilj_corrections").await?;
            let pool = db::connect(&database_url).await.unwrap();
            db::migrate(&pool).await.unwrap();
            Some((database_url, pool))
        })
        .await
        .clone()
}

/// A fresh bounded context, and the subject of a role with admin access to it.
async fn bounded_context(pool: &Pool) -> (String, String) {
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
    db::insert_role(pool, &role).await.unwrap();
    let name = format!("crm_{}", generate_token_id());
    let bc = BoundedContext {
        name: name.clone(),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    db::insert_role_access_mapping(
        pool,
        &RoleAccessMapping {
            role,
            bounded_context: bc,
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
    (name, subject)
}

fn builder(database_url: &str, bc: &str, subject: &str) -> SkiljBuilder {
    Skilj::builder(database_url)
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
        .bounded_context(bc)
        .blind_tag::<Email>()
        .blind_index_key(BlindIndexKey::from_bytes([11; 32]))
        .key_service(master_key())
        .reconciliation_role(subject)
        .event_type::<EmailClaimed>()
        .event_type::<EmailReleased>()
        .command_type::<RegisterCustomer>()
        .command_type::<CorrectEmail>()
        .command_type::<ReleaseEmail>()
        .command_type::<RetractAny>()
}

struct Crm {
    skilj: Skilj,
    pool: Pool,
    bc: String,
    cache: EventCache,
}

async fn crm() -> Option<Crm> {
    let (database_url, pool) = test_db().await?;
    let (bc, subject) = bounded_context(&pool).await;
    let (skilj, _) = builder(&database_url, &bc, &subject)
        .retractions()
        .snapshot::<ClaimCount>()
        .snapshot::<PartitionedClaimCount>()
        .command_type::<ReportClaimCount>()
        .projection::<CurrentClaims>()
        .build()
        .await
        .unwrap();
    Some(Crm {
        skilj,
        pool,
        bc,
        cache: EventCache::new(1000),
    })
}

fn kind(outcome: &SubmitCommandOutcome) -> String {
    match outcome {
        SubmitCommandOutcome::Accepted { .. } => "accepted".into(),
        SubmitCommandOutcome::Rejected { kind, .. } => kind.clone(),
        other => panic!("unexpected {other:?}"),
    }
}

fn id(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

impl Crm {
    async fn submit(&self, name: &str, payload: serde_json::Value) -> String {
        let command_type = db::get_command_type(&self.pool, &self.bc, name)
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
        kind(&outcome)
    }

    async fn register(&self, customer_id: &str, email: &str) -> String {
        self.submit(
            "RegisterCustomer",
            serde_json::json!({ "customer_id": customer_id, "email": email }),
        )
        .await
    }

    async fn correct(&self, customer_id: &str, email: &str) -> String {
        self.submit(
            "CorrectEmail",
            serde_json::json!({ "customer_id": customer_id, "email": email }),
        )
        .await
    }

    async fn retract(&self, position: i64) -> String {
        self.submit("RetractAny", serde_json::json!({ "position": position }))
            .await
    }

    async fn events(&self) -> Vec<Event> {
        db::list_events_for_bounded_context(&self.pool, &self.bc)
            .await
            .unwrap()
    }

    async fn latest_position(&self) -> i64 {
        self.events().await.last().unwrap().sequence
    }

    async fn shutdown(self) {
        self.skilj
            .shutdown(std::time::Duration::from_secs(10))
            .await;
    }
}

fn email_tag(event: &Event) -> Option<&str> {
    event
        .tags
        .iter()
        .find(|tag| tag.key == "email")
        .and_then(|tag| tag.value.as_deref())
}

/// The whole point: the old address is free and the new one taken, atomically,
/// and the retraction carries the old hash though nobody supplied the old
/// address or could compute its hash.
#[test]
fn a_correction_frees_the_old_value_and_claims_the_new_one() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let alice = id("c");
        assert_eq!(crm.register(&alice, "alcie@example.com").await, "accepted");
        let original = crm.events().await.pop().unwrap();

        assert_eq!(crm.correct(&alice, "alice@example.com").await, "accepted");

        let events = crm.events().await;
        let [_, retraction, replacement] = events.as_slice() else {
            panic!("a claim, a retraction and a claim: {events:?}");
        };
        assert_eq!(retraction.event_type.name, RETRACTED_EVENT_TYPE);
        assert_eq!(replacement.event_type.name, "EmailClaimed");
        assert_eq!(email_tag(retraction), email_tag(&original));
        assert_ne!(email_tag(replacement), email_tag(&original));
        assert!(retraction
            .tags
            .iter()
            .any(|tag| tag.key == RETRACTS_TAG_KEY
                && tag.value == Some(original.sequence.to_string())));
        assert_eq!(
            retraction.metadata.causation_id,
            replacement.metadata.causation_id
        );
        let payload: RetractionPayload = serde_json::from_str(&retraction.payload).unwrap();
        assert_eq!(payload.position, original.sequence);
        assert_eq!(payload.ground, RetractionGround::WrongDataSubmitted);
        assert_eq!(
            payload.retracted_event_type.as_deref(),
            Some("EmailClaimed")
        );
        assert!(!retraction.payload.contains("alcie"));

        assert_eq!(
            crm.register(&id("c"), "alcie@example.com").await,
            "accepted"
        );
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "email_taken"
        );
        crm.shutdown().await;
    });
}

/// Only the spelling changes, so the hash doesn't: the retraction comes first
/// and the value stays claimed.
#[test]
fn a_correction_to_the_same_normalized_value_keeps_it_taken() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let alice = id("c");
        assert_eq!(crm.register(&alice, "Alice@example.com").await, "accepted");
        assert_eq!(crm.correct(&alice, "alice@example.com").await, "accepted");
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "email_taken"
        );
        crm.shutdown().await;
    });
}

#[test]
fn a_correction_to_a_value_someone_holds_is_rejected() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let alice = id("c");
        assert_eq!(crm.register(&alice, "alice@example.com").await, "accepted");
        assert_eq!(crm.register(&id("c"), "bob@example.com").await, "accepted");
        assert_eq!(crm.correct(&alice, "bob@example.com").await, "email_taken");
        assert_eq!(
            crm.correct(&id("c"), "new@example.com").await,
            "nothing_to_correct"
        );
        crm.shutdown().await;
    });
}

/// A correction racing a new claim of its target: they conflict on the new
/// value's tag, so exactly one gets it.
#[test]
fn a_correction_racing_a_claim_of_the_target_lets_exactly_one_win() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        for round in 0..8 {
            let alice = id("c");
            let target = format!("target{round}@example.com");
            let start = format!("start{round}@example.com");
            assert_eq!(crm.register(&alice, &start).await, "accepted");
            let bob = id("c");
            let (corrected, registered) =
                tokio::join!(crm.correct(&alice, &target), crm.register(&bob, &target));
            let outcomes = [corrected, registered];
            assert_eq!(
                outcomes.iter().filter(|k| *k == "accepted").count(),
                1,
                "{outcomes:?}"
            );
            assert_eq!(
                outcomes.iter().filter(|k| *k == "email_taken").count(),
                1,
                "{outcomes:?}"
            );
        }
        crm.shutdown().await;
    });
}

/// Two corrections of one customer conflict on the customer's tag. Whichever
/// goes second sees the first's claim and retracts that, so one address stands
/// at the end and the other is free.
#[test]
fn corrections_racing_each_other_leave_one_standing_claim() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let alice = id("c");
        assert_eq!(crm.register(&alice, "start@example.com").await, "accepted");
        let (first, second) = tokio::join!(
            crm.correct(&alice, "first@example.com"),
            crm.correct(&alice, "second@example.com"),
        );
        assert_eq!((first.as_str(), second.as_str()), ("accepted", "accepted"));

        assert_eq!(
            crm.register(&id("c"), "start@example.com").await,
            "accepted"
        );
        let first_free = crm.register(&id("c"), "first@example.com").await == "accepted";
        let second_free = crm.register(&id("c"), "second@example.com").await == "accepted";
        assert_ne!(first_free, second_free, "one is held, the other free");
        crm.shutdown().await;
    });
}

/// Retraction followed by re-registration: the value is free for anyone, and
/// the earlier claim, retracted, doesn't count against the new holder.
#[test]
fn a_retracted_claim_can_be_registered_again() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        let position = crm.latest_position().await;
        assert_eq!(crm.retract(position).await, "accepted");
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "email_taken"
        );
        crm.shutdown().await;
    });
}

/// The case `.latest()` gets wrong: the retraction of an old claim is the
/// newest event carrying the value, and the value is still held.
#[test]
fn a_late_retraction_of_an_old_claim_leaves_the_current_holder() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, bob) = (id("c"), id("c"));
        assert_eq!(crm.register(&alice, "shared@example.com").await, "accepted");
        let old = crm.latest_position().await;
        assert_eq!(
            crm.submit(
                "ReleaseEmail",
                serde_json::json!({ "customer_id": alice, "email": "shared@example.com" }),
            )
            .await,
            "accepted"
        );
        assert_eq!(crm.register(&bob, "shared@example.com").await, "accepted");

        assert_eq!(crm.retract(old).await, "accepted");

        assert_eq!(
            crm.register(&id("c"), "shared@example.com").await,
            "email_taken"
        );
        crm.shutdown().await;
    });
}

#[test]
fn the_store_refuses_a_retraction_it_cannot_honour() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        assert_eq!(crm.retract(999_999).await, "retraction_target_not_found");
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        let position = crm.latest_position().await;
        assert_eq!(crm.retract(position).await, "accepted");
        assert_eq!(crm.retract(position).await, "already_retracted");
        let retraction = crm.latest_position().await;
        assert_eq!(crm.retract(retraction).await, "retraction_of_retraction");
        crm.shutdown().await;
    });
}

/// The batched path decides every command before writing any, so the second
/// retraction can't find the first in the table.
#[test]
fn a_batch_retracting_one_event_twice_accepts_one() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        let position = crm.latest_position().await;
        let command_type = db::get_command_type(&crm.pool, &crm.bc, "RetractAny")
            .await
            .unwrap()
            .unwrap();
        let items = (0..4)
            .map(|_| CommandBatchItem {
                command_type: command_type.clone(),
                payload: serde_json::json!({ "position": position }).to_string(),
                client_id: "client".to_string(),
                correlation_id: None,
                causation_id: None,
                idempotency_key: None,
            })
            .collect();
        let outcomes = CommandBatcher::new()
            .decide_and_submit_many(
                &crm.pool,
                &crm.skilj.command_dispatcher(),
                &crm.skilj.projection_dispatcher(),
                &*crm.skilj.snapshot_dispatcher(),
                &EventBroadcaster::new(16),
                &crm.cache,
                Some(&master_key()),
                &crm.bc,
                items,
                false,
            )
            .await;
        let kinds: Vec<String> = outcomes.iter().map(|o| kind(o.as_ref().unwrap())).collect();
        assert_eq!(
            kinds.iter().filter(|k| *k == "accepted").count(),
            1,
            "{kinds:?}"
        );
        assert_eq!(
            kinds.iter().filter(|k| *k == "already_retracted").count(),
            3,
            "{kinds:?}"
        );
        assert_eq!(
            crm.events()
                .await
                .iter()
                .filter(|e| e.event_type.name == RETRACTED_EVENT_TYPE)
                .count(),
            1
        );
        crm.shutdown().await;
    });
}

#[test]
fn a_dry_run_says_what_a_retraction_would_carry_and_writes_nothing() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        let original = crm.events().await.pop().unwrap();
        let command_type = db::get_command_type(&crm.pool, &crm.bc, "RetractAny")
            .await
            .unwrap()
            .unwrap();
        let dry_run = |position: i64| {
            let command_type = command_type.clone();
            let crm = &crm;
            async move {
                db::dry_run_command(
                    &crm.pool,
                    &*crm.skilj.command_dispatcher(),
                    &*crm.skilj.snapshot_dispatcher(),
                    &crm.cache,
                    &command_type,
                    &serde_json::json!({ "position": position }).to_string(),
                    "client",
                    Some(&master_key()),
                    now(),
                )
                .await
                .unwrap()
            }
        };
        let DryRunDecision::Accepted { events } = dry_run(original.sequence).await.decision else {
            panic!("the retraction would be accepted");
        };
        assert!(original.tags.iter().all(|tag| events[0].tags.contains(tag)));
        assert_eq!(crm.events().await.len(), 1);
        let DryRunDecision::Rejected { kind, .. } = dry_run(999_999).await.decision else {
            panic!("no such event");
        };
        assert_eq!(kind, "retraction_target_not_found");
        crm.shutdown().await;
    });
}

/// Forgetting a customer removes the hash from the retraction as well as from
/// the event it copied it from, so the erased address leaves no trace.
#[test]
fn erasure_reaches_the_tags_a_retraction_copied() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let alice = id("c");
        assert_eq!(crm.register(&alice, "alcie@example.com").await, "accepted");
        assert_eq!(crm.correct(&alice, "alice@example.com").await, "accepted");
        db::forget_subject_in_blind_tags(&crm.pool, &crm.bc, "customer", &alice)
            .await
            .unwrap();
        for event in crm.events().await {
            assert_eq!(email_tag(&event), None, "{event:?}");
        }
        crm.shutdown().await;
    });
}

/// A projection folding `Retractable` takes the claim out itself.
#[test]
fn a_projection_takes_a_retracted_claim_out_itself() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let alice = id("c");
        assert_eq!(crm.register(&alice, "alice@example.com").await, "accepted");
        let position = crm.latest_position().await;
        assert_eq!(crm.retract(position).await, "accepted");
        let events = crm.events().await;

        let projections = crm.skilj.projection_dispatcher();
        let mut state = "{\"position\":null}".to_string();
        for event in &events {
            for key in projections.keys(&crm.bc, "CurrentClaims", event).unwrap() {
                assert_eq!(key, alice);
                state = projections
                    .project(&crm.bc, "CurrentClaims", &state, event, &key)
                    .unwrap()
                    .unwrap();
                let current: CurrentClaim = serde_json::from_str(&state).unwrap();
                let expected = (event.event_type.name != RETRACTED_EVENT_TYPE).then_some(position);
                assert_eq!(current.position, expected);
            }
        }
        crm.shutdown().await;
    });
}

impl Crm {
    /// Registers `count` addresses for a new customer, returning it and their
    /// positions.
    async fn customer_with_claims(&self, count: usize) -> (String, Vec<i64>) {
        let customer = id("c");
        let mut positions = Vec::new();
        for _ in 0..count {
            let email = format!("{}@example.com", id("e"));
            assert_eq!(self.register(&customer, &email).await, "accepted");
            positions.push(self.latest_position().await);
        }
        (customer, positions)
    }

    async fn catch_up_snapshots(&self) {
        db::catch_up_snapshots(&self.pool, &self.bc, &*self.skilj.snapshot_dispatcher())
            .await
            .unwrap();
    }

    /// The state and position of a stored row.
    async fn stored_count(&self, snapshot: &str, customer: &str) -> (u32, i64) {
        let (_, as_of_sequence, state, _) =
            db::get_snapshot_state(&self.pool, &self.bc, snapshot, "customer", customer)
                .await
                .unwrap()
                .expect("a stored row");
        (serde_json::from_str(&state).unwrap(), as_of_sequence)
    }

    /// What a command is handed as the customer's [`ClaimCount`].
    async fn reported_count(&self, customer: &str) -> String {
        self.submit(
            "ReportClaimCount",
            serde_json::json!({ "customer_id": customer }),
        )
        .await
    }
}

/// A retraction arriving after its target was folded: catch-up refolds the row
/// without the retracted event, partitioned or not, and without handing the
/// snapshot's fold an event its enum doesn't know.
#[test]
fn catch_up_takes_a_retracted_event_out_of_a_stored_row() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, positions) = crm.customer_with_claims(3).await;
        crm.catch_up_snapshots().await;
        for snapshot in ["ClaimCount", "PartitionedClaimCount"] {
            assert_eq!(crm.stored_count(snapshot, &alice).await.0, 3);
        }

        assert_eq!(crm.retract(positions[0]).await, "accepted");
        let retraction = crm.latest_position().await;
        crm.catch_up_snapshots().await;
        for snapshot in ["ClaimCount", "PartitionedClaimCount"] {
            assert_eq!(
                crm.stored_count(snapshot, &alice).await,
                (2, retraction),
                "{snapshot}"
            );
        }

        // And it stays out when a later event folds on top.
        let email = format!("{}@example.com", id("e"));
        assert_eq!(crm.register(&alice, &email).await, "accepted");
        crm.catch_up_snapshots().await;
        for snapshot in ["ClaimCount", "PartitionedClaimCount"] {
            assert_eq!(crm.stored_count(snapshot, &alice).await.0, 3, "{snapshot}");
        }
        crm.shutdown().await;
    });
}

/// An event and its retraction in one catch-up chunk, and a customer next to
/// them untouched by either.
#[test]
fn catch_up_of_an_event_and_its_retraction_together_folds_neither() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, positions) = crm.customer_with_claims(2).await;
        let (bob, _) = crm.customer_with_claims(2).await;
        assert_eq!(crm.retract(positions[1]).await, "accepted");
        crm.catch_up_snapshots().await;
        for snapshot in ["ClaimCount", "PartitionedClaimCount"] {
            assert_eq!(crm.stored_count(snapshot, &alice).await.0, 1, "{snapshot}");
            assert_eq!(crm.stored_count(snapshot, &bob).await.0, 2, "{snapshot}");
        }
        crm.shutdown().await;
    });
}

/// A command reading a stored row catch-up hasn't brought up to date: the row
/// holds the retracted event and only the tail says so.
#[test]
fn a_command_never_decides_on_a_retracted_event_in_a_stored_row() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, positions) = crm.customer_with_claims(3).await;
        crm.catch_up_snapshots().await;
        assert_eq!(crm.reported_count(&alice).await, "count_3");

        assert_eq!(crm.retract(positions[0]).await, "accepted");
        assert_eq!(crm.stored_count("ClaimCount", &alice).await.0, 3);
        assert_eq!(crm.reported_count(&alice).await, "count_2");

        // Both the event and its retraction past the row.
        let email = format!("{}@example.com", id("e"));
        assert_eq!(crm.register(&alice, &email).await, "accepted");
        let unfolded = crm.latest_position().await;
        assert_eq!(crm.reported_count(&alice).await, "count_3");
        assert_eq!(crm.retract(unfolded).await, "accepted");
        assert_eq!(crm.reported_count(&alice).await, "count_2");
        crm.shutdown().await;
    });
}

/// A retraction committed between a command's read and its lock: nothing is
/// read under the lock, and the command goes back to its read, which gets the
/// state right.
#[test]
fn a_retraction_committed_after_the_read_sends_the_command_back() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, positions) = crm.customer_with_claims(3).await;
        crm.catch_up_snapshots().await;

        let command_type = db::get_command_type(&crm.pool, &crm.bc, "ReportClaimCount")
            .await
            .unwrap()
            .unwrap();
        let payload = serde_json::json!({ "customer_id": alice }).to_string();
        let context = skilj_core::plugin::DecisionContext::at(chrono::Utc::now());
        let resolved = db::resolve_command_submission(
            &crm.pool,
            &*crm.skilj.command_dispatcher(),
            &*crm.skilj.snapshot_dispatcher(),
            &crm.cache,
            &command_type,
            &payload,
            &context,
        )
        .await
        .unwrap();

        assert_eq!(crm.retract(positions[1]).await, "accepted");

        let error = db::submit_command(
            &crm.pool,
            &*crm.skilj.command_dispatcher(),
            &*crm.skilj.projection_dispatcher(),
            &EventBroadcaster::new(16),
            &crm.cache,
            &command_type,
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
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "snapshot_retracted_meanwhile");
        assert!(db::back_to_the_read(&error));
        assert!(skilj_retry::another_instance_can_do_it(error.code()));

        assert_eq!(crm.reported_count(&alice).await, "count_2");
        crm.shutdown().await;
    });
}

/// A retraction not yet in the state is passed over under the lock: no need
/// to go back to the read for an event that was never folded.
#[test]
fn a_retraction_of_an_event_past_the_state_stays_under_the_lock() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, _) = crm.customer_with_claims(1).await;
        crm.catch_up_snapshots().await;
        let email = format!("{}@example.com", id("e"));
        assert_eq!(crm.register(&alice, &email).await, "accepted");
        let unfolded = crm.latest_position().await;

        let command_type = db::get_command_type(&crm.pool, &crm.bc, "ReportClaimCount")
            .await
            .unwrap()
            .unwrap();
        let payload = serde_json::json!({ "customer_id": alice }).to_string();
        let context = skilj_core::plugin::DecisionContext::at(chrono::Utc::now());
        let mut resolved = db::resolve_command_submission(
            &crm.pool,
            &*crm.skilj.command_dispatcher(),
            &*crm.skilj.snapshot_dispatcher(),
            &crm.cache,
            &command_type,
            &payload,
            &context,
        )
        .await
        .unwrap();
        // As if this read had stopped at the stored row.
        let stored = crm.stored_count("ClaimCount", &alice).await;
        for state in &mut resolved.snapshot_states {
            state.state_json = stored.0.to_string();
            state.folded_through = stored.1;
        }

        assert_eq!(crm.retract(unfolded).await, "accepted");

        let outcome = db::submit_command(
            &crm.pool,
            &*crm.skilj.command_dispatcher(),
            &*crm.skilj.projection_dispatcher(),
            &EventBroadcaster::new(16),
            &crm.cache,
            &command_type,
            &payload,
            "client",
            None,
            None,
            &[],
            &resolved.consistency_tags,
            &resolved.matching_events,
            resolved.decision,
            None,
            now(),
            None,
            &resolved.snapshot_states,
            None,
            Some(stored.1),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(kind(&outcome), "count_1");
        crm.shutdown().await;
    });
}

/// One retraction racing one command, round after round: whichever way each
/// lands, at most one retraction can get in between, so a single submission
/// never surfaces the error and never reports a state that isn't real.
#[test]
fn a_command_racing_a_retraction_never_surfaces_the_retry() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, positions) = crm.customer_with_claims(24).await;
        crm.catch_up_snapshots().await;
        let mut remaining = positions.len();
        for position in positions {
            let (retracted, reported) =
                tokio::join!(crm.retract(position), crm.reported_count(&alice));
            assert_eq!(retracted, "accepted");
            let (before, after) = (
                format!("count_{remaining}"),
                format!("count_{}", remaining - 1),
            );
            assert!(
                reported == before || reported == after,
                "{reported}, expected {before} or {after}"
            );
            remaining -= 1;
        }
        crm.shutdown().await;
    });
}

/// The old snapshot API folds in the decider, out of the store's reach.
#[test]
fn build_refuses_retractions_next_to_the_old_snapshot_api() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let error = builder(&database_url, &bc, &subject)
            .retractions()
            .snapshot::<ClaimCount>()
            .command_type::<OldApiSnapshotCommand>()
            .build()
            .await
            .err()
            .expect("refused");
        assert!(
            error.to_string().contains("QueryItemMapping::snapshot"),
            "{error}"
        );
    });
}

/// A caller scoped to one customer can't retract another's event, whatever
/// position it names: a retraction frees a value. The retraction is owned too,
/// so scoped readers only see their own.
#[test]
fn an_owner_scoped_retraction_stays_with_its_owner() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let (skilj, _) = builder(&database_url, &bc, &subject)
            .retractions_owned_by("customer")
            .command_type::<RetractOwn>()
            .build()
            .await
            .unwrap();
        let crm = Crm {
            skilj,
            pool,
            bc,
            cache: EventCache::new(1000),
        };
        let (alice, bob) = (id("c"), id("c"));
        assert_eq!(crm.register(&alice, "alice@example.com").await, "accepted");
        let position = crm.latest_position().await;
        assert_eq!(
            crm.submit(
                "RetractOwn",
                serde_json::json!({ "position": position, "customer_id": bob }),
            )
            .await,
            "retraction_target_out_of_scope"
        );
        assert_eq!(
            crm.submit(
                "RetractOwn",
                serde_json::json!({ "position": position, "customer_id": alice }),
            )
            .await,
            "accepted"
        );
        let retraction = crm.events().await.pop().unwrap();
        assert_eq!(
            retraction.event_type.owner_tag_key.as_deref(),
            Some("customer")
        );
        assert!(retraction
            .tags
            .iter()
            .any(|tag| tag.key == "customer" && tag.value.as_deref() == Some(alice.as_str())));
        let payload: RetractionPayload = serde_json::from_str(&retraction.payload).unwrap();
        assert_eq!(payload.owner.as_deref(), Some(alice.as_str()));
        assert!(skilj_core::event_store::event_owner_scope_satisfied(
            &retraction,
            Some(alice.as_str())
        ));
        assert!(!skilj_core::event_store::event_owner_scope_satisfied(
            &retraction,
            Some(bob.as_str())
        ));
        crm.shutdown().await;
    });
}

struct OwnedEvent;

impl EventType for OwnedEvent {
    type Payload = EmailPayload;
    const NAME: &'static str = "OwnedEvent";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer()]
    }
    fn owner_tag_key() -> Option<&'static str> {
        Some("customer")
    }
}

/// An unowned retraction next to owned types would show a scoped reader the
/// tags of what it retracted.
#[test]
fn build_refuses_unowned_retractions_next_to_owned_types() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let error = builder(&database_url, &bc, &subject)
            .event_type::<OwnedEvent>()
            .retractions()
            .build()
            .await
            .err()
            .expect("refused");
        assert!(
            error.to_string().contains("retractions_owned_by"),
            "{error}"
        );
    });
}

struct FakeRetraction;

impl EventType for FakeRetraction {
    type Payload = EmailPayload;
    const NAME: &'static str = RETRACTED_EVENT_TYPE;
}

/// The store treats the name specially, so nothing else may take it.
#[test]
fn the_retraction_type_name_is_reserved() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let error = builder(&database_url, &bc, &subject)
            .event_type::<FakeRetraction>()
            .build()
            .await
            .err()
            .expect("refused");
        assert_eq!(error.code(), "reserved_event_type_name", "{error}");
    });
}
