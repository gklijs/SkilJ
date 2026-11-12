//! The unique-email domain of `corrections.rs`, shared with the tests that run
//! generated histories against it (issue #189): customers claiming an email
//! address that only one of them may hold, correcting it, releasing it, and an
//! operator retracting any event by position (docs/architecture.md §235).
//!
//! The domain comes in two variants with the same commands and events:
//! [`Blind`], where the email tag is a blind tag (§205), the address is an
//! encrypted field, and `decide()` sees only hashes; and [`Plain`], where the
//! tag carries the address itself. A variant is a type parameter, because a
//! command's tag mappings are associated functions with no instance to ask.
//!
//! The rules, which a generated history is checked against:
//! - a customer holds at most one address: [`RegisterCustomer`] rejects
//!   `email_taken` when someone else holds the address, and
//!   `already_holds_a_value` when the customer holds one already;
//! - [`CorrectEmail`] replaces the customer's address in one append, a
//!   retraction and a new claim, and rejects `email_taken` or
//!   `nothing_to_correct`;
//! - [`ReleaseEmail`] gives up the address the customer holds, and rejects
//!   `not_held` when the customer doesn't hold that address;
//! - [`RetractAny`] retracts whatever position it is given, so the store's
//!   own retraction checks are what answer it;
//! - [`RetractRelease`] is the operator saying a release was a mistake. It
//!   gives the address back, and so it is refused with `value_taken_since`
//!   once anyone has a standing claim of the address after the release, even
//!   one released again. Another customer's: the customer's own read doesn't
//!   see that claim, so the old claim would stand there while someone else
//!   held the address. The customer's own: both claims would stand, and a
//!   correction retracting the newer would leave the older holding the
//!   address next to the new one (found by the model, issue #189). It rejects `not_a_release` for a position that isn't this
//!   customer's release, and `already_holds_a_value` when the customer holds
//!   another address now; the store rejects `retraction_target_mismatch` for
//!   a release of another address than the payload's, which the decision
//!   reads the history of.

// Each test binary that includes this module uses a different part of it.
#![allow(dead_code)]

pub mod model;

use std::marker::PhantomData;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::retraction::{
    claim_query, corrected, correction_query, requiring_target_tags, retract, standing,
    Retractable, RetractionGround, ValueClaims, ValueEffect,
};
use skilj::{
    normalize, BlindErasure, BlindIndexKey, BlindTag, CommandType, EventType, KeyService, Skilj,
    SkiljBuilder,
};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event, EventBroadcaster};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{
    generate_token_id, CommandDecision, EventSpec, QueryItemMapping, SensitiveField, Tag,
    TagMapping,
};

/// The blind tag of the [`Blind`] variant.
pub struct Email;

impl BlindTag for Email {
    const TAG_KEY: &'static str = "email";
    const ERASURE: BlindErasure = BlindErasure::with_subject("customer");
    fn normalize(value: &str) -> String {
        normalize::email(value)
    }
}

/// How the email tag is stored: the one thing the two variants differ in.
pub trait EmailTagging: 'static {
    /// Whether the tag is blind, for a test that checks what was stored.
    const BLIND: bool;
    /// The email tag mapping every event and command of the domain uses.
    fn email() -> TagMapping;
    /// The domain's sensitive fields.
    fn sensitive_fields() -> Vec<SensitiveField>;
    /// What the builder needs for the tag beyond the domain's types.
    fn configure(builder: SkiljBuilder) -> SkiljBuilder;
}

/// The email tag is blind and the address encrypted (§205).
pub struct Blind;

impl EmailTagging for Blind {
    const BLIND: bool = true;
    fn email() -> TagMapping {
        TagMapping::blind::<Email>("email")
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        vec![SensitiveField::new("email", "customer", "customer_id")]
    }
    fn configure(builder: SkiljBuilder) -> SkiljBuilder {
        builder
            .blind_tag::<Email>()
            .blind_index_key(BlindIndexKey::from_bytes([11; 32]))
    }
}

/// The email tag carries the address as given, so nothing normalizes it.
pub struct Plain;

impl EmailTagging for Plain {
    const BLIND: bool = false;
    fn email() -> TagMapping {
        TagMapping::new("email", "email")
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        Vec::new()
    }
    fn configure(builder: SkiljBuilder) -> SkiljBuilder {
        builder
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EmailPayload {
    pub customer_id: String,
    pub email: String,
}

pub fn customer() -> TagMapping {
    TagMapping::new("customer", "customer_id")
}

pub struct EmailClaimed<V>(PhantomData<V>);

impl<V: EmailTagging> EventType for EmailClaimed<V> {
    type Payload = EmailPayload;
    const NAME: &'static str = "EmailClaimed";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), V::email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        V::sensitive_fields()
    }
}

pub struct EmailReleased<V>(PhantomData<V>);

impl<V: EmailTagging> EventType for EmailReleased<V> {
    type Payload = EmailPayload;
    const NAME: &'static str = "EmailReleased";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), V::email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        V::sensitive_fields()
    }
}

/// Which of the two it is; a decision goes by tags, not by the address.
pub enum CustomerEvent {
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

pub const CLAIMS_AND_RELEASES: [&str; 2] = ["EmailClaimed", "EmailReleased"];

pub fn claims(events: &[Retractable<CustomerEvent>]) -> ValueClaims<'_, CustomerEvent> {
    ValueClaims::fold(events, "email", |event| match event {
        CustomerEvent::Claimed => ValueEffect::Claims,
        CustomerEvent::Released => ValueEffect::Releases,
    })
}

pub fn taken() -> CommandDecision {
    CommandDecision::Rejected {
        reason: "this email address is taken".into(),
        kind: "email_taken".into(),
    }
}

pub fn claim(payload: &EmailPayload) -> EventSpec {
    EventSpec {
        event_type: "EmailClaimed".into(),
        payload: serde_json::to_value(payload).unwrap(),
    }
}

/// A defect planted in a command, for checking that the model-based tests
/// catch it (issue #189). Every command has [`NO_DEFECT`] unless a test asks.
pub const NO_DEFECT: u8 = 0;
/// [`RegisterCustomer`] doesn't hold a customer to one address.
pub const IGNORES_HELD: u8 = 1;
/// [`RegisterCustomer`]'s consistency query leaves out `EmailReleased`, so a
/// released address stays taken.
pub const IGNORES_RELEASES: u8 = 2;
/// [`RetractRelease`] gives an address back after someone else claimed it.
pub const IGNORES_CLAIMS_SINCE: u8 = 3;

/// A customer claims an address nobody holds. It reads the customer's own
/// claims as well as the address's, so a customer holds at most one.
pub struct RegisterCustomer<V, const DEFECT: u8 = NO_DEFECT>(PhantomData<V>);

impl<V: EmailTagging, const DEFECT: u8> CommandType for RegisterCustomer<V, DEFECT> {
    type Payload = EmailPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "RegisterCustomer";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), V::email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        V::sensitive_fields()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        if DEFECT == IGNORES_RELEASES {
            return correction_query(&["EmailClaimed"], customer(), V::email());
        }
        correction_query(&CLAIMS_AND_RELEASES, customer(), V::email())
    }
    fn decide(payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        let claims = claims(events);
        if claims
            .held_by_another("customer", &payload.customer_id)
            .is_some()
        {
            return taken();
        }
        if DEFECT != IGNORES_HELD && claims.held_by("customer", &payload.customer_id).is_some() {
            return CommandDecision::Rejected {
                reason: "this customer holds an address already; correct it instead".into(),
                kind: "already_holds_a_value".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![claim(payload)],
        }
    }
}

pub struct CorrectEmail<V>(PhantomData<V>);

impl<V: EmailTagging> CommandType for CorrectEmail<V> {
    type Payload = EmailPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "CorrectEmail";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), V::email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        V::sensitive_fields()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        correction_query(&CLAIMS_AND_RELEASES, customer(), V::email())
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
/// isn't the customer's any more. Only the holder can release it.
pub struct ReleaseEmail<V>(PhantomData<V>);

impl<V: EmailTagging> CommandType for ReleaseEmail<V> {
    type Payload = EmailPayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "ReleaseEmail";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), V::email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        V::sensitive_fields()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        claim_query(&CLAIMS_AND_RELEASES, V::email())
    }
    fn decide(payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        // The read holds only this address's events, so a claim the customer
        // holds among them is a claim of this address.
        if claims(events)
            .held_by("customer", &payload.customer_id)
            .is_none()
        {
            return CommandDecision::Rejected {
                reason: "this customer doesn't hold this address".into(),
                kind: "not_held".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "EmailReleased".into(),
                payload: serde_json::to_value(payload).unwrap(),
            }],
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RetractPayload {
    pub position: i64,
    #[serde(default)]
    pub customer_id: Option<String>,
}

/// Retracts whatever position it is given, the store being what checks it.
pub struct RetractAny;

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

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RetractReleasePayload {
    pub position: i64,
    pub customer_id: String,
    /// The released address, which the decision reads the history of.
    pub email: String,
}

/// An operator retracting a release that should not have happened, giving the
/// address back; see the module doc for when it is refused.
pub struct RetractRelease<V, const DEFECT: u8 = NO_DEFECT>(PhantomData<V>);

impl<V: EmailTagging, const DEFECT: u8> CommandType for RetractRelease<V, DEFECT> {
    type Payload = RetractReleasePayload;
    type Event = Retractable<CustomerEvent>;
    const NAME: &'static str = "RetractRelease";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), V::email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        V::sensitive_fields()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        correction_query(&CLAIMS_AND_RELEASES, customer(), V::email())
    }
    fn decide(payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        let own = |tags: &[Tag]| {
            tags.iter().any(|tag| {
                tag.key == "customer" && tag.value.as_deref() == Some(&payload.customer_id)
            })
        };
        let standing = standing(events);
        let Some(address) = standing.iter().find_map(|event| {
            (event.position == payload.position
                && matches!(event.event, CustomerEvent::Released)
                && own(event.tags))
            .then(|| email_value(event.tags))
            .flatten()
        }) else {
            return CommandDecision::Rejected {
                reason: "no standing release of an address by this customer at that position"
                    .into(),
                kind: "not_a_release".into(),
            };
        };
        // A claim of this address after the release, the customer's own too.
        // Another customer's events are read only through the payload's
        // address, which the store checks is the release's.
        if DEFECT != IGNORES_CLAIMS_SINCE
            && standing.iter().any(|event| {
                event.position > payload.position
                    && matches!(event.event, CustomerEvent::Claimed)
                    && email_value(event.tags) == Some(address)
            })
        {
            return CommandDecision::Rejected {
                reason: "this address was claimed again after it was released".into(),
                kind: "value_taken_since".into(),
            };
        }
        if let Some(current) = claims(events).held_by("customer", &payload.customer_id) {
            if email_value(current.tags) != Some(address) {
                return CommandDecision::Rejected {
                    reason: "this customer holds another address now; release it first".into(),
                    kind: "already_holds_a_value".into(),
                };
            }
        }
        // The address read is the payload's; the store checks the release is
        // of that address, comparing hashes where a blind tag leaves this
        // decision unable to.
        CommandDecision::Accepted {
            events: vec![requiring_target_tags(
                retract(
                    payload.position,
                    RetractionGround::ProcessedWrongly,
                    "released by mistake",
                ),
                ["email"],
            )],
        }
    }
}

/// The email tag's value: the address for [`Plain`], its hash for [`Blind`].
fn email_value(tags: &[Tag]) -> Option<&str> {
    tags.iter()
        .find(|tag| tag.key == "email")
        .and_then(|tag| tag.value.as_deref())
}

pub fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

pub fn now() -> chrono::DateTime<chrono::Utc> {
    use chrono::SubsecRound;
    chrono::Utc::now().trunc_subsecs(6)
}

pub fn master_key() -> KeyService {
    KeyService::new(skilj_core::encryption::InMemoryKeyProvider::from_bytes(
        [7; 32],
    ))
}

/// A fresh bounded context, and the subject of a role with admin access to it.
/// Every bounded context has a schema of its own, so its events, its recorded
/// idempotency keys and its blind index are those of nothing else.
pub async fn bounded_context(pool: &Pool) -> (String, String) {
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

/// The domain's types for `bc`, in variant `V`.
pub fn builder<V: EmailTagging>(database_url: &str, bc: &str, subject: &str) -> SkiljBuilder {
    builder_with_defect::<V>(database_url, bc, subject, NO_DEFECT)
}

/// [`builder`] with `defect` planted in the command it names.
pub fn builder_with_defect<V: EmailTagging>(
    database_url: &str,
    bc: &str,
    subject: &str,
    defect: u8,
) -> SkiljBuilder {
    let builder = Skilj::builder(database_url)
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
        .bounded_context(bc);
    let builder = V::configure(builder)
        .key_service(master_key())
        .reconciliation_role(subject)
        .event_type::<EmailClaimed<V>>()
        .event_type::<EmailReleased<V>>()
        .command_type::<CorrectEmail<V>>()
        .command_type::<ReleaseEmail<V>>()
        .command_type::<RetractAny>();
    let builder = match defect {
        IGNORES_HELD => builder.command_type::<RegisterCustomer<V, IGNORES_HELD>>(),
        IGNORES_RELEASES => builder.command_type::<RegisterCustomer<V, IGNORES_RELEASES>>(),
        _ => builder.command_type::<RegisterCustomer<V>>(),
    };
    match defect {
        IGNORES_CLAIMS_SINCE => builder.command_type::<RetractRelease<V, IGNORES_CLAIMS_SINCE>>(),
        _ => builder.command_type::<RetractRelease<V>>(),
    }
}

/// What a submission goes to: one built deployment of one bounded context.
#[derive(Clone, Copy)]
pub struct Target<'a> {
    pub skilj: &'a Skilj,
    pub pool: &'a Pool,
    pub bc: &'a str,
    pub cache: &'a EventCache,
}

impl Target<'_> {
    /// A submission as an application makes one: `decide()` then the store,
    /// with the caller's `client_id` and idempotency key.
    pub async fn submit(
        self,
        command_type: &str,
        payload: serde_json::Value,
        client_id: &str,
        idempotency_key: Option<&str>,
    ) -> SubmitCommandOutcome {
        let command_type = db::get_command_type(self.pool, self.bc, command_type)
            .await
            .unwrap()
            .unwrap();
        db::decide_and_submit_command(
            self.pool,
            &*self.skilj.command_dispatcher(),
            &*self.skilj.projection_dispatcher(),
            &*self.skilj.snapshot_dispatcher(),
            &EventBroadcaster::new(16),
            self.cache,
            &command_type,
            &payload.to_string(),
            client_id,
            None,
            None,
            Some(&master_key()),
            now(),
            idempotency_key,
            None,
        )
        .await
        .unwrap()
    }
}
