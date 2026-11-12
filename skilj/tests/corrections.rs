//! Retractions and corrections (docs/architecture.md §235, issue #133),
//! against Postgres, on the case that makes them hard: a unique email address
//! that is a blind tag, so `decide()` can't compare values and the old value
//! has to be released by tags alone.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::retraction::{
    claim_query, doubt, lift_doubt, open_doubts, requiring_target_tags, retract, DoubtPayload,
    Retractable, RetractionGround, RetractionPayload, DOUBTED_EVENT_TYPE, DOUBTS_TAG_KEY,
    DOUBT_EVENT_TYPES, DOUBT_LIFTED_EVENT_TYPE, RETRACTED_EVENT_TYPE, RETRACTS_TAG_KEY,
};
use skilj::{BlindIndexKey, CommandType, EventType, Projection, Skilj, SkiljBuilder, Snapshot};
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
use skilj_test_fixture::corrections::{all_races, UniqueValueCommands};

mod unique_crm;

use unique_crm::{
    bounded_context, claim, claims, customer, master_key, now, runtime, taken, Blind, Email,
    EmailPayload, EmailTagging, RetractAny, RetractPayload, CLAIMS_AND_RELEASES,
};

// This file's tests are of the blind variant.
type EmailClaimed = unique_crm::EmailClaimed<Blind>;
use unique_crm::CustomerEvent;

fn email() -> TagMapping {
    Blind::email()
}

fn encrypted_email() -> Vec<SensitiveField> {
    Blind::sensitive_fields()
}

fn builder(database_url: &str, bc: &str, subject: &str) -> SkiljBuilder {
    unique_crm::builder::<Blind>(database_url, bc, subject).command_type::<ImportClaim>()
}

/// Claims an address with only the address's own rule, so one customer can
/// hold several: a history imported from elsewhere, for the snapshot tests
/// below, which count a customer's claims. [`RegisterCustomer`] holds a
/// customer to one.
struct ImportClaim;

impl CommandType for ImportClaim {
    type Payload = EmailPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "ImportClaim";
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
        .doubts()
        .command_type::<DoubtAny>()
        .command_type::<LiftDoubt>()
        .command_type::<ReportDoubts>()
        .command_type::<StanceRequiring>()
        .command_type::<DoubtAndRetract>()
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
        kind(&self.submit_keyed(name, payload, None).await)
    }

    async fn submit_keyed(
        &self,
        name: &str,
        payload: serde_json::Value,
        idempotency_key: Option<&str>,
    ) -> SubmitCommandOutcome {
        unique_crm::Target {
            skilj: &self.skilj,
            pool: &self.pool,
            bc: &self.bc,
            cache: &self.cache,
        }
        .submit(name, payload, "client", idempotency_key)
        .await
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

    async fn release(&self, customer_id: &str, email: &str) -> String {
        self.submit(
            "ReleaseEmail",
            serde_json::json!({ "customer_id": customer_id, "email": email }),
        )
        .await
    }

    async fn retract_release(&self, customer_id: &str, email: &str, position: i64) -> String {
        self.submit(
            "RetractRelease",
            serde_json::json!({ "customer_id": customer_id, "email": email, "position": position }),
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

/// A customer holds one address: a second registration is refused, and once
/// the first is released or retracted the customer can register again.
#[test]
fn a_customer_holds_at_most_one_address() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let alice = id("c");
        assert_eq!(crm.register(&alice, "first@example.com").await, "accepted");
        assert_eq!(
            crm.register(&alice, "second@example.com").await,
            "already_holds_a_value"
        );
        assert_eq!(
            crm.register(&alice, "first@example.com").await,
            "already_holds_a_value",
            "the customer's own address is held, but not by another"
        );
        assert_eq!(crm.release(&alice, "first@example.com").await, "accepted");
        assert_eq!(crm.register(&alice, "second@example.com").await, "accepted");
        let second = crm.latest_position().await;
        assert_eq!(crm.retract(second).await, "accepted");
        assert_eq!(crm.register(&alice, "third@example.com").await, "accepted");
        crm.shutdown().await;
    });
}

/// Only the holder releases an address, and only the one it holds.
#[test]
fn a_release_of_an_address_the_customer_does_not_hold_is_rejected() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, bob) = (id("c"), id("c"));
        assert_eq!(crm.register(&alice, "alice@example.com").await, "accepted");
        assert_eq!(crm.release(&bob, "alice@example.com").await, "not_held");
        assert_eq!(crm.release(&alice, "other@example.com").await, "not_held");
        assert_eq!(
            crm.release(&alice, "Alice@Example.com").await,
            "accepted",
            "the same address once normalized"
        );
        assert_eq!(crm.release(&alice, "alice@example.com").await, "not_held");
        assert_eq!(crm.register(&bob, "alice@example.com").await, "accepted");
        crm.shutdown().await;
    });
}

/// `ProcessCommand`: a key is matched on its scope alone, the payload is not
/// compared. A second submission under a key carrying another payload is
/// answered with the first's events, and what it asked for doesn't happen.
#[test]
fn a_reused_key_with_another_payload_answers_with_the_first_submissions_events() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, bob) = (id("c"), id("c"));
        let register = |customer: &str, email: &str| {
            serde_json::json!({ "customer_id": customer, "email": email })
        };
        let SubmitCommandOutcome::Accepted { events, .. } = crm
            .submit_keyed(
                "RegisterCustomer",
                register(&alice, "alice@example.com"),
                Some("signup-1"),
            )
            .await
        else {
            panic!("a first registration is accepted");
        };
        let first: Vec<i64> = events.iter().map(|event| event.sequence).collect();

        let second = crm
            .submit_keyed(
                "RegisterCustomer",
                register(&bob, "bob@example.com"),
                Some("signup-1"),
            )
            .await;
        let SubmitCommandOutcome::Deduplicated {
            triggered_event_sequences,
        } = second
        else {
            panic!("the key was accepted before, so this is a duplicate: {second:?}");
        };
        assert_eq!(triggered_event_sequences, first);
        assert_eq!(crm.events().await.len(), 1, "nothing was appended");
        assert_eq!(
            crm.register(&id("c"), "bob@example.com").await,
            "accepted",
            "bob's address was never claimed"
        );
        crm.shutdown().await;
    });
}

/// A retracted release gives the address back, so it is refused once anyone
/// has claimed the address since, even when that claim was released again:
/// the release's customer would otherwise see the old claim standing, next to
/// another's or next to their own.
#[test]
fn a_release_is_retracted_only_while_nobody_has_claimed_the_address_since() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, bob) = (id("c"), id("c"));
        assert_eq!(crm.register(&alice, "kept@example.com").await, "accepted");
        assert_eq!(crm.release(&alice, "kept@example.com").await, "accepted");
        let release = crm.latest_position().await;
        assert_eq!(
            crm.retract_release(&alice, "kept@example.com", release)
                .await,
            "accepted"
        );
        assert_eq!(
            crm.register(&bob, "kept@example.com").await,
            "email_taken",
            "alice holds it again"
        );
        assert_eq!(
            crm.retract_release(&alice, "kept@example.com", release)
                .await,
            "not_a_release",
            "the release no longer stands"
        );
        assert_eq!(crm.release(&alice, "kept@example.com").await, "accepted");
        let again = crm.latest_position().await;
        assert_eq!(crm.register(&alice, "kept@example.com").await, "accepted");
        assert_eq!(
            crm.retract_release(&alice, "kept@example.com", again).await,
            "value_taken_since",
            "alice claimed it again herself: two claims would stand"
        );

        let (carol, dave) = (id("c"), id("c"));
        assert_eq!(crm.register(&carol, "lost@example.com").await, "accepted");
        assert_eq!(crm.release(&carol, "lost@example.com").await, "accepted");
        let lost = crm.latest_position().await;
        assert_eq!(crm.register(&dave, "lost@example.com").await, "accepted");
        assert_eq!(
            crm.retract_release(&carol, "lost@example.com", lost).await,
            "value_taken_since"
        );
        assert_eq!(crm.release(&dave, "lost@example.com").await, "accepted");
        assert_eq!(
            crm.retract_release(&carol, "lost@example.com", lost).await,
            "value_taken_since",
            "the address is free again, but dave's claim still stands"
        );
        assert_eq!(
            crm.register(&carol, "new@example.com").await,
            "accepted",
            "carol holds nothing"
        );
        crm.shutdown().await;
    });
}

/// Giving an address back can't leave a customer holding two, and only the
/// customer's own release of that address can be retracted this way.
#[test]
fn a_release_retraction_is_refused_when_it_would_not_be_one() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (alice, bob) = (id("c"), id("c"));
        assert_eq!(crm.register(&alice, "first@example.com").await, "accepted");
        let claim = crm.latest_position().await;
        assert_eq!(
            crm.retract_release(&alice, "first@example.com", claim)
                .await,
            "not_a_release",
            "a claim isn't a release"
        );
        assert_eq!(crm.release(&alice, "first@example.com").await, "accepted");
        let release = crm.latest_position().await;
        assert_eq!(
            crm.retract_release(&bob, "first@example.com", release)
                .await,
            "not_a_release",
            "nor is someone else's release"
        );
        assert_eq!(
            crm.retract_release(&alice, "other@example.com", release)
                .await,
            "retraction_target_mismatch",
            "nor a release of another address, which the store sees by its hash"
        );
        assert_eq!(crm.register(&alice, "second@example.com").await, "accepted");
        assert_eq!(
            crm.retract_release(&alice, "first@example.com", release)
                .await,
            "already_holds_a_value"
        );
        crm.shutdown().await;
    });
}

/// `requiring_target_tags`: the store compares the command's own tag with the
/// target's, a blind one by its hash, which a decision can't compute.
#[test]
fn a_stance_requiring_target_tags_is_refused_on_another_value() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let alice = id("c");
        assert_eq!(crm.register(&alice, "alice@example.com").await, "accepted");
        let claim = crm.latest_position().await;
        let stance = |email: &str, doubt: bool, key: &str| {
            serde_json::json!({
                "position": claim,
                "customer_id": alice,
                "email": email,
                "doubt": doubt,
                "key": key,
            })
        };
        for (doubt, mismatch) in [
            (true, "doubt_target_mismatch"),
            (false, "retraction_target_mismatch"),
        ] {
            assert_eq!(
                crm.submit("StanceRequiring", stance("bob@example.com", doubt, "email"))
                    .await,
                mismatch
            );
        }
        let invalid = [(true, "invalid_doubt"), (false, "invalid_retraction")];
        for (doubt, kind) in invalid {
            assert_eq!(
                crm.submit(
                    "StanceRequiring",
                    stance("alice@example.com", doubt, "skilj_retracts")
                )
                .await,
                kind,
                "the command has no such tag to compare"
            );
        }
        assert_eq!(
            crm.submit(
                "StanceRequiring",
                stance("Alice@Example.com", false, "email")
            )
            .await,
            "accepted",
            "the same address once normalized has the same hash"
        );
        let events = crm.events().await;
        let retraction = events.last().unwrap();
        assert_eq!(retraction.event_type.name, "EventRetracted");
        assert!(
            !retraction.payload.contains("target_tags"),
            "the condition isn't stored: {}",
            retraction.payload
        );
        crm.shutdown().await;
    });
}

/// The same domain with an ordinary email tag: the rules hold as they do for
/// a blind one, the address is stored as given, and since nothing normalizes
/// it, another spelling is another address.
#[test]
fn the_plain_variant_keeps_the_rules_and_stores_the_address() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let (skilj, _) = unique_crm::builder::<unique_crm::Plain>(&database_url, &bc, &subject)
            .retractions()
            .build()
            .await
            .unwrap();
        let cache = EventCache::new(1000);
        let submit = |command: &'static str, customer: String, email: &'static str| {
            let (skilj, pool, bc, cache) = (&skilj, &pool, &bc, &cache);
            async move {
                kind(
                    &unique_crm::Target {
                        skilj,
                        pool,
                        bc,
                        cache,
                    }
                    .submit(
                        command,
                        serde_json::json!({ "customer_id": customer, "email": email }),
                        "client",
                        None,
                    )
                    .await,
                )
            }
        };
        let (alice, bob) = (id("c"), id("c"));
        assert_eq!(
            submit("RegisterCustomer", alice.clone(), "alice@example.com").await,
            "accepted"
        );
        assert_eq!(
            submit("RegisterCustomer", bob.clone(), "alice@example.com").await,
            "email_taken"
        );
        assert_eq!(
            submit("RegisterCustomer", bob.clone(), "Alice@example.com").await,
            "accepted"
        );
        assert_eq!(
            submit("CorrectEmail", alice.clone(), "alice@example.org").await,
            "accepted"
        );
        assert_eq!(
            submit("ReleaseEmail", alice.clone(), "alice@example.com").await,
            "not_held"
        );
        let events = db::list_events_for_bounded_context(&pool, &bc)
            .await
            .unwrap();
        assert_eq!(events.len(), 4, "two claims, a retraction and a claim");
        assert_eq!(email_tag(&events[3]), Some("alice@example.org"));
        skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

impl UniqueValueCommands for Crm {
    async fn claim(&self, identity: &str, value: &str) -> Result<(), String> {
        accepted(self.register(identity, value).await)
    }

    async fn correct(&self, identity: &str, value: &str) -> Result<(), String> {
        accepted(Crm::correct(self, identity, value).await)
    }

    async fn retract_claim(&self, identity: &str) -> Result<(), String> {
        let position =
            self.events()
                .await
                .iter()
                .rfind(|event| {
                    event.event_type.name == "EmailClaimed"
                        && event.tags.iter().any(|tag| {
                            tag.key == "customer" && tag.value.as_deref() == Some(identity)
                        })
                })
                .map(|event| event.sequence)
                .ok_or("nothing_to_retract")?;
        accepted(self.retract(position).await)
    }
}

fn accepted(kind: String) -> Result<(), String> {
    if kind == "accepted" {
        Ok(())
    } else {
        Err(kind)
    }
}

/// The published scenarios (`skilj_test_fixture::corrections`), against the
/// store: a correction racing a claim of its target, corrections racing each
/// other, and a retraction followed by a new claim.
#[test]
fn the_published_correction_races_hold_against_the_store() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        all_races(&crm).await;

        // The scenario allows an application to reject the second of two
        // corrections. The store's own retry goes further: it decides again, on
        // what the first left, and both go through.
        let alice = id("c");
        assert_eq!(crm.register(&alice, "start@example.com").await, "accepted");
        let (first, second) = tokio::join!(
            Crm::correct(&crm, &alice, "first@example.com"),
            Crm::correct(&crm, &alice, "second@example.com"),
        );
        assert_eq!((first.as_str(), second.as_str()), ("accepted", "accepted"));
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
    /// One more address for `customer`, however many it holds.
    async fn import(&self, customer: &str, email: &str) -> String {
        self.submit(
            "ImportClaim",
            serde_json::json!({ "customer_id": customer, "email": email }),
        )
        .await
    }

    /// Imports `count` addresses for a new customer, returning it and their
    /// positions.
    async fn customer_with_claims(&self, count: usize) -> (String, Vec<i64>) {
        let customer = id("c");
        let mut positions = Vec::new();
        for _ in 0..count {
            let email = format!("{}@example.com", id("e"));
            assert_eq!(self.import(&customer, &email).await, "accepted");
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
        assert_eq!(crm.import(&alice, &email).await, "accepted");
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
        assert_eq!(crm.import(&alice, &email).await, "accepted");
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
        assert_eq!(crm.import(&alice, &email).await, "accepted");
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

/// Folds the stored address into its state, the ciphertext being all a fold
/// ever gets.
#[derive(Debug)]
struct StoredAddress(String);

impl BoundedContextEvent for StoredAddress {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        (event.event_type.name == "EmailClaimed").then(|| {
            serde_json::from_str::<EmailPayload>(&event.payload)
                .map(|payload| StoredAddress(payload.email))
        })
    }
}

struct LastAddress;

impl Snapshot for LastAddress {
    type State = String;
    type Event = StoredAddress;
    const NAME: &'static str = "LastAddress";
    const TAG_KEY: &'static str = "customer";
    const VERSION: u64 = 1;
    fn fold(state: &mut Self::State, event: &Self::Event) {
        state.clone_from(&event.0);
    }
}

/// A row refolded from history after `forgetSubject`, here because of a
/// retraction, is folded from what's stored: the forgotten address can't come
/// back through it. The issue asks this of a rebuild; `projection_query.rs`
/// covers a projection's.
#[test]
fn a_snapshot_refolded_after_erasure_holds_no_plaintext() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let (skilj, _) = builder(&database_url, &bc, &subject)
            .retractions()
            .snapshot::<LastAddress>()
            .build()
            .await
            .unwrap();
        let crm = Crm {
            skilj,
            pool,
            bc,
            cache: EventCache::new(1000),
        };
        let (customer, positions) = crm.customer_with_claims(2).await;
        crm.catch_up_snapshots().await;
        let stored = |crm: &Crm, customer: String| {
            let (pool, bc) = (crm.pool.clone(), crm.bc.clone());
            async move {
                let (_, _, state, _) =
                    db::get_snapshot_state(&pool, &bc, "LastAddress", "customer", &customer)
                        .await
                        .unwrap()
                        .expect("a stored row");
                serde_json::from_str::<String>(&state).unwrap()
            }
        };
        assert!(!stored(&crm, customer.clone()).await.contains('@'));

        db::forget_subject_in_blind_tags(&crm.pool, &crm.bc, "customer", &customer)
            .await
            .unwrap();
        db::destroy_encryption_key(&crm.pool, &crm.bc, "customer", &customer, now())
            .await
            .unwrap();

        // Retracting the event the row holds makes catch-up start it over.
        assert_eq!(crm.retract(positions[1]).await, "accepted");
        crm.catch_up_snapshots().await;

        let first = crm
            .events()
            .await
            .into_iter()
            .find(|event| event.sequence == positions[0])
            .unwrap();
        let first: EmailPayload = serde_json::from_str(&first.payload).unwrap();
        let state = stored(&crm, customer).await;
        assert_eq!(state, first.email, "the fold of the event still standing");
        assert!(!state.contains('@'), "{state}");
        crm.shutdown().await;
    });
}

/// What a retraction does to work its target already started: nothing. The
/// store has no way from a retraction to a deadline or a route's delivery, so
/// whether it matters is up to the commands they submit (docs/architecture.md
/// §235).
mod side_effects {
    use super::*;
    use skilj::retraction::standing;
    use skilj::{CancelDeadline, CrossContextRoute, ScheduleDeadline};
    use skilj_core::plugin::DeadlineSpec;
    use skilj_core::shared::Tag;

    const SHOP: &str = "skilj_corrections_shop";
    const STOCK: &str = "skilj_corrections_stock";

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct Order {
        order_id: String,
        deadline_at: chrono::DateTime<chrono::Utc>,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct OrderId {
        order_id: String,
    }

    fn order_tag() -> TagMapping {
        TagMapping::new("order", "order_id")
    }

    macro_rules! shop_event {
        ($name:ident, $payload:ty, $bc:expr) => {
            struct $name;
            impl EventType for $name {
                type Payload = $payload;
                const NAME: &'static str = stringify!($name);
                const BOUNDED_CONTEXT: &'static str = $bc;
                fn tag_mappings() -> Vec<TagMapping> {
                    vec![order_tag()]
                }
            }
        };
    }

    shop_event!(OrderPlaced, Order, SHOP);
    shop_event!(OrderWithdrawn, OrderId, SHOP);
    shop_event!(OrderExpired, OrderId, SHOP);
    shop_event!(OrderExpiredAnyway, OrderId, SHOP);
    shop_event!(StockReserved, OrderId, STOCK);

    /// An order, to whichever command folds it.
    struct Placed;

    impl BoundedContextEvent for Placed {
        fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
            (event.event_type.name == "OrderPlaced").then_some(Ok(Placed))
        }
    }

    fn emit<P: Serialize>(event_type: &str, payload: &P) -> EventSpec {
        EventSpec {
            event_type: event_type.into(),
            payload: serde_json::to_value(payload).unwrap(),
        }
    }

    struct PlaceOrder;

    impl CommandType for PlaceOrder {
        type Payload = Order;
        type Event = Placed;
        const NAME: &'static str = "PlaceOrder";
        const BOUNDED_CONTEXT: &'static str = SHOP;
        fn tag_mappings() -> Vec<TagMapping> {
            vec![order_tag()]
        }
        fn consistency_query() -> Vec<QueryItemMapping> {
            vec![QueryItemMapping::nothing()]
        }
        fn decide(payload: &Order, _: &[Placed]) -> CommandDecision {
            CommandDecision::Accepted {
                events: vec![emit("OrderPlaced", payload)],
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct Withdrawal {
        order_id: String,
        position: i64,
    }

    fn withdrawn_by_retraction(payload: &Withdrawal) -> EventSpec {
        retract(
            payload.position,
            RetractionGround::WrongDataSubmitted,
            "never ordered",
        )
    }

    /// A bare retraction, which nothing downstream can act on.
    struct WithdrawOrder;

    impl CommandType for WithdrawOrder {
        type Payload = Withdrawal;
        type Event = Placed;
        const NAME: &'static str = "WithdrawOrder";
        const BOUNDED_CONTEXT: &'static str = SHOP;
        fn tag_mappings() -> Vec<TagMapping> {
            vec![order_tag()]
        }
        fn consistency_query() -> Vec<QueryItemMapping> {
            vec![QueryItemMapping::nothing()]
        }
        fn decide(payload: &Withdrawal, _: &[Placed]) -> CommandDecision {
            CommandDecision::Accepted {
                events: vec![withdrawn_by_retraction(payload)],
            }
        }
    }

    /// A retraction next to a domain event, which carries what a reactor
    /// needs: a retraction's payload has no tags to cancel by.
    struct WithdrawOrderAndSay;

    impl CommandType for WithdrawOrderAndSay {
        type Payload = Withdrawal;
        type Event = Placed;
        const NAME: &'static str = "WithdrawOrderAndSay";
        const BOUNDED_CONTEXT: &'static str = SHOP;
        fn tag_mappings() -> Vec<TagMapping> {
            vec![order_tag()]
        }
        fn consistency_query() -> Vec<QueryItemMapping> {
            vec![QueryItemMapping::nothing()]
        }
        fn decide(payload: &Withdrawal, _: &[Placed]) -> CommandDecision {
            CommandDecision::Accepted {
                events: vec![
                    withdrawn_by_retraction(payload),
                    emit(
                        "OrderWithdrawn",
                        &OrderId {
                            order_id: payload.order_id.clone(),
                        },
                    ),
                ],
            }
        }
    }

    /// Fired by a deadline, and checks the order still stands.
    struct ExpireOrder;

    impl CommandType for ExpireOrder {
        type Payload = OrderId;
        type Event = Retractable<Placed>;
        const NAME: &'static str = "ExpireOrder";
        const BOUNDED_CONTEXT: &'static str = SHOP;
        fn tag_mappings() -> Vec<TagMapping> {
            vec![order_tag()]
        }
        fn consistency_query() -> Vec<QueryItemMapping> {
            claim_query(&["OrderPlaced"], order_tag())
        }
        fn decide(payload: &OrderId, events: &[Self::Event]) -> CommandDecision {
            if standing(events).is_empty() {
                return CommandDecision::Rejected {
                    reason: "the order was withdrawn".into(),
                    kind: "order_withdrawn".into(),
                };
            }
            CommandDecision::Accepted {
                events: vec![emit("OrderExpired", payload)],
            }
        }
    }

    /// Fired by a deadline, and looks at nothing.
    struct ExpireOrderAnyway;

    impl CommandType for ExpireOrderAnyway {
        type Payload = OrderId;
        type Event = Placed;
        const NAME: &'static str = "ExpireOrderAnyway";
        const BOUNDED_CONTEXT: &'static str = SHOP;
        fn tag_mappings() -> Vec<TagMapping> {
            vec![order_tag()]
        }
        fn consistency_query() -> Vec<QueryItemMapping> {
            vec![QueryItemMapping::nothing()]
        }
        fn decide(payload: &OrderId, _: &[Placed]) -> CommandDecision {
            CommandDecision::Accepted {
                events: vec![emit("OrderExpiredAnyway", payload)],
            }
        }
    }

    struct ReserveStock;

    impl CommandType for ReserveStock {
        type Payload = OrderId;
        type Event = Placed;
        const NAME: &'static str = "ReserveStock";
        const BOUNDED_CONTEXT: &'static str = STOCK;
        fn tag_mappings() -> Vec<TagMapping> {
            vec![order_tag()]
        }
        fn consistency_query() -> Vec<QueryItemMapping> {
            vec![QueryItemMapping::nothing()]
        }
        fn decide(payload: &OrderId, _: &[Placed]) -> CommandDecision {
            CommandDecision::Accepted {
                events: vec![emit("StockReserved", payload)],
            }
        }
    }

    fn deadline(order: &Order) -> DeadlineSpec<OrderId> {
        DeadlineSpec {
            fire_at: order.deadline_at,
            expires_at: None,
            tags: vec![Tag {
                key: "order".into(),
                value: Some(order.order_id.clone()),
            }],
            payload: OrderId {
                order_id: order.order_id.clone(),
            },
            recurrence: None,
        }
    }

    struct ExpireDeadline;

    impl ScheduleDeadline for ExpireDeadline {
        type Source = OrderPlaced;
        type Target = ExpireOrder;
        const NAME: &'static str = "ExpireDeadline";
        fn schedule(order: &Order) -> Option<DeadlineSpec<OrderId>> {
            Some(deadline(order))
        }
    }

    struct ExpireAnywayDeadline;

    impl ScheduleDeadline for ExpireAnywayDeadline {
        type Source = OrderPlaced;
        type Target = ExpireOrderAnyway;
        const NAME: &'static str = "ExpireAnywayDeadline";
        fn schedule(order: &Order) -> Option<DeadlineSpec<OrderId>> {
            Some(deadline(order))
        }
    }

    struct CancelExpireAnyway;

    impl CancelDeadline for CancelExpireAnyway {
        type Source = OrderWithdrawn;
        type Deadline = ExpireAnywayDeadline;
        const NAME: &'static str = "CancelExpireAnyway";
        fn cancel_tags(withdrawn: &OrderId) -> Option<Vec<Tag>> {
            Some(vec![Tag {
                key: "order".into(),
                value: Some(withdrawn.order_id.clone()),
            }])
        }
    }

    struct ReserveOnOrder;

    impl CrossContextRoute for ReserveOnOrder {
        type Source = OrderPlaced;
        type Target = ReserveStock;
        const NAME: &'static str = "ReserveOnOrder";
        fn route(order: &Order) -> Option<OrderId> {
            Some(OrderId {
                order_id: order.order_id.clone(),
            })
        }
    }

    async fn insert_context(pool: &Pool, name: &str, role: &Role) {
        let bounded_context = BoundedContext {
            name: name.to_string(),
            status: BoundedContextStatus::Active,
            created_at: now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(pool, &bounded_context)
            .await
            .unwrap();
        db::insert_role_access_mapping(
            pool,
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
    }

    fn shop_builder(database_url: &str, subject: &str) -> SkiljBuilder {
        Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(8))
            .key_service(master_key())
            .reconciliation_role(subject)
            .bounded_context(SHOP)
            .retractions()
            .event_type::<OrderPlaced>()
            .event_type::<OrderWithdrawn>()
            .event_type::<OrderExpired>()
            .event_type::<OrderExpiredAnyway>()
            .command_type::<PlaceOrder>()
            .command_type::<WithdrawOrder>()
            .command_type::<WithdrawOrderAndSay>()
            .command_type::<ExpireOrder>()
            .command_type::<ExpireOrderAnyway>()
            .bounded_context(STOCK)
            .event_type::<StockReserved>()
            .command_type::<ReserveStock>()
    }

    fn orders_of(events: &[Event], event_type: &str) -> Vec<String> {
        let mut orders: Vec<String> = events
            .iter()
            .filter(|event| event.event_type.name == event_type)
            .map(|event| {
                serde_json::from_str::<OrderId>(&event.payload)
                    .unwrap()
                    .order_id
            })
            .collect();
        orders.sort();
        orders
    }

    impl Crm {
        async fn place(&self, order_id: &str, deadline_at: chrono::DateTime<chrono::Utc>) -> i64 {
            let outcome = self
                .submit(
                    "PlaceOrder",
                    serde_json::json!({ "order_id": order_id, "deadline_at": deadline_at }),
                )
                .await;
            assert_eq!(outcome, "accepted");
            self.latest_position().await
        }

        async fn withdraw(&self, command: &str, order_id: &str, position: i64) {
            let outcome = self
                .submit(
                    command,
                    serde_json::json!({ "order_id": order_id, "position": position }),
                )
                .await;
            assert_eq!(outcome, "accepted");
        }

        async fn pending_deadlines(&self) -> i64 {
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM \"bc_{SHOP}\".deadlines \
                 WHERE status IN ('pending', 'firing')"
            )))
            .fetch_one(&self.pool)
            .await
            .unwrap()
        }
    }

    #[test]
    fn a_retraction_leaves_what_its_target_started_to_the_commands_involved() {
        runtime().block_on(async {
            let Some((database_url, pool)) = test_db().await else {
                return;
            };
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
            insert_context(&pool, SHOP, &role).await;
            insert_context(&pool, STOCK, &role).await;

            // Withdrawn before anything reacting to it exists: the reactors
            // start from the beginning, and it's in the history they replay.
            let (skilj, _) = shop_builder(&database_url, &subject).build().await.unwrap();
            let before = Crm {
                skilj,
                pool: pool.clone(),
                bc: SHOP.to_string(),
                cache: EventCache::new(1000),
            };
            let far_off = now() + chrono::Duration::hours(1);
            let position = before.place("replayed", far_off).await;
            before.withdraw("WithdrawOrder", "replayed", position).await;
            before.shutdown().await;

            let (skilj, _) = shop_builder(&database_url, &subject)
                .bounded_context(SHOP)
                .schedule_deadline::<ExpireDeadline>()
                .schedule_deadline::<ExpireAnywayDeadline>()
                .cancel_deadline::<CancelExpireAnyway>()
                .deadline_poll_interval(std::time::Duration::from_millis(30))
                .cross_context_route::<ReserveOnOrder>()
                .cross_context_route_poll_interval(std::time::Duration::from_millis(30))
                .build()
                .await
                .unwrap();
            let shop = Crm {
                skilj,
                pool: pool.clone(),
                bc: SHOP.to_string(),
                cache: EventCache::new(1000),
            };

            let stock_reserved = |pool: Pool| async move {
                orders_of(
                    &db::list_events_for_bounded_context(&pool, STOCK)
                        .await
                        .unwrap(),
                    "StockReserved",
                )
            };

            // Far enough off for each order to be routed and withdrawn first.
            let due = now() + chrono::Duration::seconds(3);
            let standing_order = shop.place("standing", due).await;
            let bare = shop.place("withdrawn", due).await;
            let announced = shop.place("announced", due).await;
            assert!(standing_order < bare && bare < announced);
            let mut routed = false;
            for _ in 0..100 {
                if stock_reserved(pool.clone()).await.len() == 4 {
                    routed = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(routed, "{:?}", stock_reserved(pool.clone()).await);

            shop.withdraw("WithdrawOrder", "withdrawn", bare).await;
            shop.withdraw("WithdrawOrderAndSay", "announced", announced)
                .await;

            // `replayed`'s deadlines are an hour off, and fire now only so
            // this test doesn't have to wait.
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE \"bc_{SHOP}\".deadlines SET fire_at = $1 \
                 WHERE fire_at > $1 AND status = 'pending'"
            )))
            .bind(due)
            .execute(&pool)
            .await
            .unwrap();

            let mut settled = false;
            for _ in 0..300 {
                if now() > due && shop.pending_deadlines().await == 0 {
                    settled = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(settled, "{} still pending", shop.pending_deadlines().await);

            let events = shop.events().await;
            // A route's delivery stands, and a replay delivers a withdrawn order.
            assert_eq!(
                stock_reserved(pool.clone()).await,
                ["announced", "replayed", "standing", "withdrawn"]
            );
            // A deadline fires, and a target that looks refuses by itself.
            assert_eq!(orders_of(&events, "OrderExpired"), ["standing"]);
            // One that doesn't look goes ahead, unless a domain event next to
            // the retraction cancelled it.
            assert_eq!(
                orders_of(&events, "OrderExpiredAnyway"),
                ["replayed", "standing", "withdrawn"]
            );
            shop.shutdown().await;
        });
    }
}

/// Doubts whatever position it is given, the store being what checks it.
struct DoubtAny;

impl CommandType for DoubtAny {
    type Payload = RetractPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "DoubtAny";
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::nothing()]
    }
    fn decide(payload: &Self::Payload, _events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![doubt(payload.position, "under investigation")],
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct StanceRequiringPayload {
    position: i64,
    customer_id: String,
    email: String,
    doubt: bool,
    key: String,
}

/// A retraction or a doubt of `position` that requires its target to carry
/// the command's email tag, or whichever `key` names, for the store to check.
struct StanceRequiring;

impl CommandType for StanceRequiring {
    type Payload = StanceRequiringPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "StanceRequiring";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        encrypted_email()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::nothing()]
    }
    fn decide(payload: &Self::Payload, _events: &[Self::Event]) -> CommandDecision {
        let stance = if payload.doubt {
            doubt(payload.position, "is this the address?")
        } else {
            retract(
                payload.position,
                RetractionGround::WrongDataSubmitted,
                "wrong address",
            )
        };
        CommandDecision::Accepted {
            events: vec![requiring_target_tags(stance, [payload.key.as_str()])],
        }
    }
}

/// [`DoubtAny`] for a caller scoped to one customer.
struct DoubtOwn;

impl CommandType for DoubtOwn {
    type Payload = RetractPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "DoubtOwn";
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
        DoubtAny::decide(payload, events)
    }
}

struct LiftDoubt;

impl CommandType for LiftDoubt {
    type Payload = RetractPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "LiftDoubt";
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::nothing()]
    }
    fn decide(payload: &Self::Payload, _events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![lift_doubt(payload.position, "looked fine after all")],
        }
    }
}

/// Doubts and retracts one position in one decision, in that order.
struct DoubtAndRetract;

impl CommandType for DoubtAndRetract {
    type Payload = RetractPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "DoubtAndRetract";
    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::nothing()]
    }
    fn decide(payload: &Self::Payload, _events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![
                retract(payload.position, RetractionGround::ProcessedWrongly, "gone"),
                doubt(payload.position, "too late"),
            ],
        }
    }
}

/// Rejects with what `open_doubts` makes of the customer's history, for a test
/// to read, and with the same query a decision acting on doubt would use.
struct ReportDoubts;

impl CommandType for ReportDoubts {
    type Payload = CountPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "ReportDoubts";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer()]
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        claim_query(
            &["EmailClaimed", DOUBT_EVENT_TYPES[0], DOUBT_EVENT_TYPES[1]],
            customer(),
        )
    }
    fn decide(_payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        let mut open: Vec<i64> = open_doubts(events).into_keys().collect();
        open.sort_unstable();
        CommandDecision::Rejected {
            reason: "reporting".into(),
            kind: format!("doubted_{open:?}"),
        }
    }
}

impl Crm {
    async fn doubt(&self, position: i64) -> String {
        self.submit("DoubtAny", serde_json::json!({ "position": position }))
            .await
    }

    async fn lift(&self, position: i64) -> String {
        self.submit("LiftDoubt", serde_json::json!({ "position": position }))
            .await
    }

    async fn doubts_of(&self, customer: &str) -> String {
        self.submit(
            "ReportDoubts",
            serde_json::json!({ "customer_id": customer }),
        )
        .await
    }

    /// Registers an address for a new customer, returning both and its position.
    async fn claimed(&self) -> (String, String, i64) {
        let customer = id("c");
        let email = format!("{}@example.com", id("e"));
        assert_eq!(self.register(&customer, &email).await, "accepted");
        (customer, email, self.latest_position().await)
    }
}

/// The whole cycle, as a decision sees it, with the store's refusals of what
/// doesn't fit the state at each step.
#[test]
fn a_doubt_opens_lifts_and_can_open_again() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (customer, _, position) = crm.claimed().await;
        assert_eq!(crm.doubts_of(&customer).await, "doubted_[]");
        assert_eq!(crm.lift(position).await, "no_open_doubt");

        assert_eq!(crm.doubt(position).await, "accepted");
        assert_eq!(
            crm.doubts_of(&customer).await,
            format!("doubted_[{position}]")
        );
        assert_eq!(crm.doubt(position).await, "already_doubted");

        assert_eq!(crm.lift(position).await, "accepted");
        assert_eq!(crm.doubts_of(&customer).await, "doubted_[]");
        assert_eq!(crm.lift(position).await, "no_open_doubt");

        assert_eq!(crm.doubt(position).await, "accepted");
        assert_eq!(
            crm.doubts_of(&customer).await,
            format!("doubted_[{position}]")
        );
        crm.shutdown().await;
    });
}

/// Like a retraction it carries its target's tags, a blind hash included, so
/// every query that found the event finds the doubt. Nothing else changes: the
/// address stays taken, since a doubted event is still endorsed.
#[test]
fn a_doubt_carries_the_tags_and_leaves_the_value_held() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (customer, email, position) = crm.claimed().await;
        let original = crm.events().await.pop().unwrap();
        assert_eq!(crm.doubt(position).await, "accepted");
        let doubted = crm.events().await.pop().unwrap();

        assert_eq!(doubted.event_type.name, DOUBTED_EVENT_TYPE);
        assert!(email_tag(&doubted).is_some());
        assert_eq!(email_tag(&doubted), email_tag(&original));
        assert!(doubted
            .tags
            .iter()
            .any(|tag| tag.key == DOUBTS_TAG_KEY && tag.value == Some(position.to_string())));
        let payload: DoubtPayload = serde_json::from_str(&doubted.payload).unwrap();
        assert_eq!(payload.position, position);
        assert_eq!(payload.doubted_event_type.as_deref(), Some("EmailClaimed"));

        assert_eq!(crm.register(&id("c"), &email).await, "email_taken");

        // Erasure reaches the copies, as it does a retraction's.
        assert_eq!(crm.lift(position).await, "accepted");
        db::forget_subject_in_blind_tags(&crm.pool, &crm.bc, "customer", &customer)
            .await
            .unwrap();
        for event in crm.events().await {
            if event.sequence >= position {
                assert_eq!(email_tag(&event), None, "{event:?}");
            }
        }
        crm.shutdown().await;
    });
}

/// A doubt usually ends in a retraction or a correction, which ends it with no
/// lift. After that there's nothing left to doubt.
#[test]
fn a_retraction_closes_a_doubt() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (customer, email, position) = crm.claimed().await;
        assert_eq!(crm.doubt(position).await, "accepted");
        assert_eq!(crm.retract(position).await, "accepted");
        assert_eq!(crm.doubts_of(&customer).await, "doubted_[]");
        assert_eq!(crm.doubt(position).await, "doubt_target_retracted");
        assert_eq!(crm.lift(position).await, "doubt_target_retracted");
        assert_eq!(crm.register(&id("c"), &email).await, "accepted");

        // A correction is a retraction, with the same effect.
        let (customer, _, position) = crm.claimed().await;
        assert_eq!(crm.doubt(position).await, "accepted");
        assert_eq!(
            crm.correct(&customer, "fixed@example.com").await,
            "accepted"
        );
        assert_eq!(crm.doubts_of(&customer).await, "doubted_[]");
        assert_eq!(crm.lift(position).await, "doubt_target_retracted");

        // In one decision too.
        let (_, _, position) = crm.claimed().await;
        assert_eq!(
            crm.submit(
                "DoubtAndRetract",
                serde_json::json!({ "position": position })
            )
            .await,
            "doubt_target_retracted"
        );
        crm.shutdown().await;
    });
}

/// What a doubt or a retraction can't be about.
#[test]
fn the_store_refuses_a_doubt_it_cannot_honour() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (_, _, position) = crm.claimed().await;
        assert_eq!(crm.doubt(position + 1000).await, "doubt_target_not_found");
        assert_eq!(crm.lift(position + 1000).await, "doubt_target_not_found");

        assert_eq!(crm.doubt(position).await, "accepted");
        let doubted = crm.latest_position().await;
        assert_eq!(crm.doubt(doubted).await, "doubt_of_stance");
        assert_eq!(crm.lift(doubted).await, "doubt_of_stance");
        assert_eq!(crm.retract(doubted).await, "retraction_of_doubt");

        assert_eq!(crm.lift(position).await, "accepted");
        let lifted = crm.latest_position().await;
        assert_eq!(crm.doubt(lifted).await, "doubt_of_stance");
        assert_eq!(crm.retract(lifted).await, "retraction_of_doubt");

        assert_eq!(crm.retract(position).await, "accepted");
        let retraction = crm.latest_position().await;
        assert_eq!(crm.doubt(retraction).await, "doubt_of_stance");
        crm.shutdown().await;
    });
}

/// A batch decides everything before writing anything, so a later item has to
/// see an earlier item's doubt and lift.
#[test]
fn a_batch_sees_its_own_doubts() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (_, _, position) = crm.claimed().await;
        let item = |command_type: &skilj_core::event_store::CommandType| CommandBatchItem {
            command_type: command_type.clone(),
            payload: serde_json::json!({ "position": position }).to_string(),
            client_id: "client".to_string(),
            correlation_id: None,
            causation_id: None,
            idempotency_key: None,
        };
        let command = |name: &'static str| {
            let (pool, bc) = (crm.pool.clone(), crm.bc.clone());
            async move {
                db::get_command_type(&pool, &bc, name)
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        let (doubt, lift, retraction) = (
            command("DoubtAny").await,
            command("LiftDoubt").await,
            command("RetractAny").await,
        );
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
                vec![
                    item(&lift),
                    item(&doubt),
                    item(&doubt),
                    item(&lift),
                    item(&lift),
                    item(&doubt),
                    item(&retraction),
                    item(&doubt),
                    item(&lift),
                ],
                false,
            )
            .await;
        let kinds: Vec<String> = outcomes.iter().map(|o| kind(o.as_ref().unwrap())).collect();
        assert_eq!(
            kinds,
            [
                "no_open_doubt",
                "accepted",
                "already_doubted",
                "accepted",
                "no_open_doubt",
                "accepted",
                "accepted",
                "doubt_target_retracted",
                "doubt_target_retracted",
            ]
        );
        crm.shutdown().await;
    });
}

/// Concurrent doubts of one event: the lock orders them, and one gets through.
#[test]
fn doubts_racing_each_other_accept_one() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (_, _, position) = crm.claimed().await;
        let (first, second) = tokio::join!(crm.doubt(position), crm.doubt(position));
        let mut kinds = [first, second];
        kinds.sort();
        assert_eq!(kinds, ["accepted", "already_doubted"]);
        crm.shutdown().await;
    });
}

/// The marker doesn't touch a snapshot: no refold and no change in the state,
/// stored or read, and the row keeps moving past a doubt.
#[test]
fn a_doubt_leaves_a_snapshot_alone() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (customer, positions) = crm.customer_with_claims(2).await;
        crm.catch_up_snapshots().await;
        assert_eq!(crm.stored_count("ClaimCount", &customer).await.0, 2);
        assert_eq!(crm.doubt(positions[0]).await, "accepted");
        assert_eq!(crm.reported_count(&customer).await, "count_2");
        crm.catch_up_snapshots().await;
        for snapshot in ["ClaimCount", "PartitionedClaimCount"] {
            let (count, as_of) = crm.stored_count(snapshot, &customer).await;
            assert_eq!(count, 2, "{snapshot}");
            assert!(as_of >= positions[1], "{snapshot} at {as_of}");
        }
        assert_eq!(crm.lift(positions[0]).await, "accepted");
        assert_eq!(crm.reported_count(&customer).await, "count_2");
        crm.shutdown().await;
    });
}

/// A row refolded from history after a doubt, here because of a retraction of
/// another event, passes the doubt over and keeps the event in doubt.
#[test]
fn a_refold_passes_a_doubt_over_and_keeps_its_target() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (customer, positions) = crm.customer_with_claims(3).await;
        crm.catch_up_snapshots().await;
        assert_eq!(crm.doubt(positions[0]).await, "accepted");
        assert_eq!(crm.retract(positions[2]).await, "accepted");
        crm.catch_up_snapshots().await;
        for snapshot in ["ClaimCount", "PartitionedClaimCount"] {
            assert_eq!(
                crm.stored_count(snapshot, &customer).await.0,
                2,
                "{snapshot}"
            );
        }
        assert_eq!(crm.reported_count(&customer).await, "count_2");
        crm.shutdown().await;
    });
}

/// With no consistency query of its own, a command reads every event carrying
/// its tags, doubts included, in an event enum that has never heard of them.
#[test]
fn a_command_reading_everything_under_a_tag_ignores_doubts_it_does_not_know() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let (skilj, _) = builder(&database_url, &bc, &subject)
            .retractions()
            .doubts()
            .command_type::<DoubtAny>()
            .command_type::<PlainTagged>()
            .build()
            .await
            .unwrap();
        let crm = Crm {
            skilj,
            pool,
            bc,
            cache: EventCache::new(1000),
        };
        let (customer, _, position) = crm.claimed().await;
        assert_eq!(crm.doubt(position).await, "accepted");
        assert_eq!(
            crm.submit(
                "PlainTagged",
                serde_json::json!({ "customer_id": customer })
            )
            .await,
            "saw_1"
        );
        crm.shutdown().await;
    });
}

struct PlainTagged;

impl CommandType for PlainTagged {
    type Payload = CountPayload;
    type Event = CustomerEvent;
    const NAME: &'static str = "PlainTagged";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer()]
    }
    fn decide(_payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Rejected {
            reason: "reporting".into(),
            kind: format!("saw_{}", events.len()),
        }
    }
}

/// `snapshot()` hands its decider the events past the stored row, a doubt among
/// them, so it's refused here as it is next to retractions.
#[test]
fn build_refuses_doubts_next_to_the_old_snapshot_api() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let error = Skilj::builder(&database_url)
            .bounded_context(&bc)
            .key_service(master_key())
            .reconciliation_role(&subject)
            .event_type::<EmailClaimed>()
            .blind_tag::<Email>()
            .blind_index_key(BlindIndexKey::from_bytes([11; 32]))
            .doubts()
            .snapshot::<ClaimCount>()
            .command_type::<OldApiSnapshotCommand>()
            .build()
            .await
            .err()
            .expect("refused");
        assert!(error.to_string().contains("snapshot()"), "{error}");
    });
}

/// What the tools show, however far past its target a retraction landed.
#[test]
fn event_stances_say_what_was_said_about_each_event() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (_, positions) = crm.customer_with_claims(4).await;
        let [plain, doubted, lifted, retracted] = positions[..] else {
            panic!("four claims");
        };
        assert_eq!(crm.doubt(doubted).await, "accepted");
        let doubt = crm.latest_position().await;
        assert_eq!(crm.doubt(lifted).await, "accepted");
        assert_eq!(crm.lift(lifted).await, "accepted");
        assert_eq!(crm.doubt(retracted).await, "accepted");
        assert_eq!(crm.retract(retracted).await, "accepted");
        let retraction = crm.latest_position().await;

        let stances = db::event_stances(&crm.pool, &crm.bc, &positions)
            .await
            .unwrap();
        assert!(!stances.contains_key(&plain));
        assert_eq!(stances[&doubted].doubted_by, Some(doubt));
        assert_eq!(stances[&doubted].retracted_by, None);
        assert_eq!(stances[&lifted].doubted_by, None);
        assert_eq!(stances[&retracted].doubted_by, None);
        assert_eq!(stances[&retracted].retracted_by, Some(retraction));
        crm.shutdown().await;
    });
}

/// A dry run answers the same, and writes nothing.
#[test]
fn a_dry_run_says_what_a_doubt_would_carry() {
    runtime().block_on(async {
        let Some(crm) = crm().await else {
            return;
        };
        let (_, _, position) = crm.claimed().await;
        let dry_run = |name: &'static str| {
            let crm = &crm;
            async move {
                let command_type = db::get_command_type(&crm.pool, &crm.bc, name)
                    .await
                    .unwrap()
                    .unwrap();
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
        let DryRunDecision::Rejected { kind, .. } = dry_run("LiftDoubt").await.decision else {
            panic!("nothing to lift");
        };
        assert_eq!(kind, "no_open_doubt");
        let DryRunDecision::Accepted { events } = dry_run("DoubtAny").await.decision else {
            panic!("a doubt is accepted");
        };
        assert!(events[0]
            .tags
            .iter()
            .any(|tag| tag.key == DOUBTS_TAG_KEY && tag.value == Some(position.to_string())));
        assert_eq!(crm.latest_position().await, position);
        crm.shutdown().await;
    });
}

#[test]
fn an_owner_scoped_doubt_stays_with_its_owner() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let (skilj, _) = builder(&database_url, &bc, &subject)
            .retractions_owned_by("customer")
            .doubts_owned_by("customer")
            .command_type::<DoubtOwn>()
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
                "DoubtOwn",
                serde_json::json!({ "position": position, "customer_id": bob }),
            )
            .await,
            "doubt_target_out_of_scope"
        );
        assert_eq!(
            crm.submit(
                "DoubtOwn",
                serde_json::json!({ "position": position, "customer_id": alice }),
            )
            .await,
            "accepted"
        );
        let doubted = crm.events().await.pop().unwrap();
        assert_eq!(
            doubted.event_type.owner_tag_key.as_deref(),
            Some("customer")
        );
        let payload: DoubtPayload = serde_json::from_str(&doubted.payload).unwrap();
        assert_eq!(payload.owner.as_deref(), Some(alice.as_str()));
        assert!(skilj_core::event_store::event_owner_scope_satisfied(
            &doubted,
            Some(alice.as_str())
        ));
        assert!(!skilj_core::event_store::event_owner_scope_satisfied(
            &doubted,
            Some(bob.as_str())
        ));
        crm.shutdown().await;
    });
}

/// An unowned doubt next to owned types would show a scoped reader the tags of
/// what it's about, as with a retraction.
#[test]
fn build_refuses_unowned_doubts_next_to_owned_types() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let error = builder(&database_url, &bc, &subject)
            .event_type::<OwnedEvent>()
            .retractions_owned_by("customer")
            .doubts()
            .build()
            .await
            .err()
            .expect("refused");
        assert!(error.to_string().contains("doubts_owned_by"), "{error}");
    });
}

struct FakeDoubt;

impl EventType for FakeDoubt {
    type Payload = EmailPayload;
    const NAME: &'static str = DOUBTED_EVENT_TYPE;
}

struct FakeLift;

impl EventType for FakeLift {
    type Payload = EmailPayload;
    const NAME: &'static str = DOUBT_LIFTED_EVENT_TYPE;
}

#[test]
fn the_doubt_type_names_are_reserved() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;
        let error = builder(&database_url, &bc, &subject)
            .retractions()
            .event_type::<FakeDoubt>()
            .build()
            .await
            .err()
            .expect("refused");
        assert_eq!(error.code(), "reserved_event_type_name", "{error}");
        let (bc, subject) = bounded_context(&pool).await;
        let error = builder(&database_url, &bc, &subject)
            .retractions()
            .event_type::<FakeLift>()
            .build()
            .await
            .err()
            .expect("refused");
        assert_eq!(error.code(), "reserved_event_type_name", "{error}");
    });
}
