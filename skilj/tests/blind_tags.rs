//! Blind-index tags (docs/architecture.md §205): an email address,
//! encrypted in every payload, is still unique per bounded context. Its
//! tag is a keyed hash of the normalized value, so the consistency query
//! of `RegisterCustomer` finds an earlier claim of the same address,
//! however it was written, without the address being stored anywhere in
//! the clear. Forgetting the customer frees the address and removes the
//! hash when the `BlindTag` says so, and keeps it taken when it doesn't.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use skilj::{
    normalize, BlindErasure, BlindIndexKey, BlindTag, CommandType, EncryptionMasterKey, EventType,
    IdpConfig, SigningAlgorithm, Skilj, SkiljBuilder,
};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::error::SkiljRejection;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event, EventBroadcaster};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{
    generate_token_id, CommandDecision, EventSpec, QueryItemMapping, SensitiveField, TagMapping,
};
use tower::ServiceExt;

/// The address is erased with the customer it belongs to.
struct ErasedEmail;

impl BlindTag for ErasedEmail {
    const TAG_KEY: &'static str = "email";
    const ERASURE: BlindErasure = BlindErasure::with_subject("customer");
    fn normalize(value: &str) -> String {
        normalize::email(value)
    }
}

/// The address stays taken after the customer is forgotten.
struct KeptEmail;

impl BlindTag for KeptEmail {
    const TAG_KEY: &'static str = "email";
    const ERASURE: BlindErasure = BlindErasure::Keep;
    fn normalize(value: &str) -> String {
        normalize::email(value)
    }
}

/// [`ErasedEmail`] with a normalizer that changed.
struct RenormalizedEmail;

impl BlindTag for RenormalizedEmail {
    const TAG_KEY: &'static str = "email";
    const NORMALIZER_VERSION: u32 = 2;
    const ERASURE: BlindErasure = BlindErasure::Keep;
    fn normalize(value: &str) -> String {
        normalize::email(value).replace('.', "")
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
    TagMapping::blind::<ErasedEmail>("email")
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

/// Only which of the two it is matters to a decision.
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

/// The latest claim or release of the payload's address.
fn latest_of_email(field: &str) -> Vec<QueryItemMapping> {
    vec![QueryItemMapping::types(&["EmailClaimed", "EmailReleased"])
        .tagged(vec![TagMapping::blind::<ErasedEmail>(field)])
        .latest()]
}

fn taken(events: &[CustomerEvent]) -> bool {
    matches!(events.last(), Some(CustomerEvent::Claimed))
}

fn emit(event_type: &str, customer_id: &str, email: &str) -> EventSpec {
    EventSpec {
        event_type: event_type.into(),
        payload: serde_json::json!({ "customer_id": customer_id, "email": email }),
    }
}

struct RegisterCustomer;

impl CommandType for RegisterCustomer {
    type Payload = EmailPayload;
    type Event = CustomerEvent;
    const NAME: &'static str = "RegisterCustomer";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        encrypted_email()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        latest_of_email("email")
    }
    fn decide(payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        if taken(events) {
            return CommandDecision::Rejected {
                reason: "this email address is taken".into(),
                kind: "email_taken".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![emit("EmailClaimed", &payload.customer_id, &payload.email)],
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct ChangeEmailPayload {
    customer_id: String,
    old_email: String,
    email: String,
}

struct ChangeEmail;

impl CommandType for ChangeEmail {
    type Payload = ChangeEmailPayload;
    type Event = CustomerEvent;
    const NAME: &'static str = "ChangeEmail";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![customer(), email()]
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        ["old_email", "email"]
            .map(|field| SensitiveField::new(field, "customer", "customer_id"))
            .to_vec()
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        latest_of_email("email")
    }
    fn decide(payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
        if taken(events) {
            return CommandDecision::Rejected {
                reason: "this email address is taken".into(),
                kind: "email_taken".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![
                emit("EmailReleased", &payload.customer_id, &payload.old_email),
                emit("EmailClaimed", &payload.customer_id, &payload.email),
            ],
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

fn master_key() -> EncryptionMasterKey {
    EncryptionMasterKey::from_bytes([7; 32])
}

fn blind_key() -> BlindIndexKey {
    BlindIndexKey::from_bytes([11; 32])
}

static TEST_DB: tokio::sync::OnceCell<Option<(String, Pool)>> = tokio::sync::OnceCell::const_new();

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(|| async {
            let database_url = skilj_test_support::database_url("skilj_blind_tags").await?;
            let pool = db::connect(&database_url).await.unwrap();
            db::migrate(&pool).await.unwrap();
            Some((database_url, pool))
        })
        .await
        .clone()
}

/// A fresh bounded context, and the subject of a role with admin access
/// to it for reconciliation.
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
            status: RoleStatus::Active,
            created_at: now(),
            revoked_at: None,
        },
    )
    .await
    .unwrap();
    (name, subject)
}

/// `Skilj::builder` with a small pool: the tests run in parallel, several
/// instances each, against one server.
fn small_builder(database_url: impl Into<String>) -> SkiljBuilder {
    skilj::Skilj::builder(database_url)
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
}

/// The customer types, registered in `bc` - without a `BlindTag`, which
/// each test adds.
fn customer_types(database_url: &str, bc: &str, subject: &str) -> SkiljBuilder {
    customer_instance(database_url, bc).reconciliation_role(subject)
}

/// [`customer_types`] for an instance that doesn't reconcile.
fn customer_instance(database_url: &str, bc: &str) -> SkiljBuilder {
    small_builder(database_url)
        .bounded_context(bc)
        .event_type::<EmailClaimed>()
        .event_type::<EmailReleased>()
        .command_type::<RegisterCustomer>()
        .command_type::<ChangeEmail>()
        .encryption_master_key(master_key())
}

struct Crm {
    skilj: Skilj,
    pool: Pool,
    bc: String,
    cache: EventCache,
    /// The reconciliation role's subject, with admin access to `bc`.
    subject: String,
}

/// A fresh bounded context with the customer types, the address erased
/// with its customer unless `B` says otherwise. `None` without a test
/// database.
async fn crm<B: BlindTag + 'static>() -> Option<Crm> {
    crm_with::<B>(|builder| builder).await
}

/// `crm()`, with a hook to adjust the builder before `build()`.
async fn crm_with<B: BlindTag + 'static>(
    configure: impl FnOnce(SkiljBuilder) -> SkiljBuilder,
) -> Option<Crm> {
    let (database_url, pool) = test_db().await?;
    let (bc, subject) = bounded_context(&pool).await;
    let builder = customer_types(&database_url, &bc, &subject)
        .blind_tag::<B>()
        .blind_index_key(blind_key());
    let (skilj, _) = configure(builder).build().await.unwrap();
    Some(Crm {
        skilj,
        pool,
        bc,
        cache: EventCache::new(1000),
        subject,
    })
}

impl Crm {
    /// The same bounded context, served by `skilj`.
    fn served_by(&self, skilj: Skilj) -> Crm {
        Crm {
            skilj,
            pool: self.pool.clone(),
            bc: self.bc.clone(),
            cache: EventCache::new(1000),
            subject: self.subject.clone(),
        }
    }

    async fn submit_through(
        &self,
        skilj: &Skilj,
        name: &str,
        payload: serde_json::Value,
    ) -> skilj_core::error::Result<SubmitCommandOutcome> {
        let command_type = db::get_command_type(&self.pool, &self.bc, name)
            .await
            .unwrap()
            .unwrap();
        db::decide_and_submit_command(
            &self.pool,
            &*skilj.command_dispatcher(),
            &*skilj.projection_dispatcher(),
            &*skilj.snapshot_dispatcher(),
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
    }

    async fn register(&self, customer_id: &str, email: &str) -> String {
        let payload = serde_json::json!({ "customer_id": customer_id, "email": email });
        kind(
            &self
                .submit_through(&self.skilj, "RegisterCustomer", payload)
                .await
                .unwrap(),
        )
    }

    async fn change(&self, customer_id: &str, old_email: &str, email: &str) -> String {
        let payload = serde_json::json!({
            "customer_id": customer_id, "old_email": old_email, "email": email,
        });
        kind(
            &self
                .submit_through(&self.skilj, "ChangeEmail", payload)
                .await
                .unwrap(),
        )
    }

    /// Every hash the bounded context stores under the `email` key.
    async fn stored_hashes(&self) -> Vec<String> {
        let schema = format!("\"bc_{}\"", self.bc);
        let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT DISTINCT tag_value FROM {schema}.event_tags \
             WHERE tag_key = 'email' AND tag_value IS NOT NULL ORDER BY 1"
        )))
        .fetch_all(&self.pool)
        .await
        .unwrap();
        rows.into_iter().map(|(h,)| h).collect()
    }

    /// Whether `needle` appears anywhere the bounded context stores tags,
    /// queries or payloads.
    async fn stores(&self, needle: &str) -> bool {
        let schema = format!("\"bc_{}\"", self.bc);
        let (found,): (bool,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT EXISTS (SELECT 1 FROM {schema}.events \
                WHERE tags::text LIKE $1 OR payload LIKE $1) \
             OR EXISTS (SELECT 1 FROM {schema}.event_tags WHERE tag_value LIKE $1) \
             OR EXISTS (SELECT 1 FROM {schema}.commands WHERE consistency_tags::text LIKE $1 \
                OR consistency_query::text LIKE $1 OR payload LIKE $1)"
        )))
        .bind(format!("%{needle}%"))
        .fetch_one(&self.pool)
        .await
        .unwrap();
        found
    }

    async fn forget(&self, customer_id: &str) {
        db::forget_subject_in_blind_tags(&self.pool, &self.bc, "customer", customer_id)
            .await
            .unwrap();
        db::destroy_encryption_key(&self.pool, &self.bc, "customer", customer_id, now())
            .await
            .unwrap();
    }
}

fn kind(outcome: &SubmitCommandOutcome) -> String {
    match outcome {
        SubmitCommandOutcome::Accepted { .. } => "accepted".into(),
        SubmitCommandOutcome::Rejected { kind, .. } => kind.clone(),
        SubmitCommandOutcome::Deduplicated { .. } => "deduplicated".into(),
    }
}

fn id(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

/// An address is taken once, however it is cased or padded, and is
/// stored nowhere in the clear: not in a tag, a query or a payload. Its
/// tags are 64-character hashes.
#[test]
fn an_address_is_taken_once_and_stored_only_as_a_hash() {
    runtime().block_on(async {
        let Some(crm) = crm::<ErasedEmail>().await else {
            return;
        };
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        assert_eq!(
            crm.register(&id("c"), "  Alice@Example.COM ").await,
            "email_taken"
        );
        assert_eq!(crm.register(&id("c"), "bob@example.com").await, "accepted");

        assert!(!crm.stores("alice").await);
        assert!(!crm.stores("example.com").await);
        let hashes = crm.stored_hashes().await;
        assert_eq!(hashes.len(), 2);
        assert!(hashes
            .iter()
            .all(|h| h.len() == 64 && h.chars().all(|c| c.is_ascii_hexdigit())));
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// Registrations of one address racing each other: the consistency query
/// makes them conflict on the hashed tag, so exactly one is accepted.
#[test]
fn concurrent_registrations_of_one_address_accept_exactly_one() {
    runtime().block_on(async {
        let Some(crm) = crm::<ErasedEmail>().await else {
            return;
        };
        let crm = std::sync::Arc::new(crm);
        let attempts = (0..8).map(|i| {
            let crm = crm.clone();
            tokio::spawn(async move {
                crm.register(&id("c"), &format!("Racer@Example.com{}", " ".repeat(i)))
                    .await
            })
        });
        let outcomes: Vec<String> = futures_util::future::join_all(attempts)
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();
        assert_eq!(outcomes.iter().filter(|k| *k == "accepted").count(), 1);
        assert_eq!(outcomes.iter().filter(|k| *k == "email_taken").count(), 7);
        let crm = std::sync::Arc::into_inner(crm).unwrap();
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// A changed address is released: someone else can take the old one, and
/// nobody the new one.
#[test]
fn changing_an_address_frees_the_old_one() {
    runtime().block_on(async {
        let Some(crm) = crm::<ErasedEmail>().await else {
            return;
        };
        let c1 = id("c");
        assert_eq!(crm.register(&c1, "old@example.com").await, "accepted");
        assert_eq!(
            crm.change(&c1, "old@example.com", "new@example.com").await,
            "accepted"
        );
        assert_eq!(
            crm.register(&id("c"), "new@example.com").await,
            "email_taken"
        );
        assert_eq!(crm.register(&id("c"), "old@example.com").await, "accepted");
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// `BlindErasure::WithSubject`: forgetting the customer removes the hash
/// from its events and commands - and from the event cache, which served
/// the registrations before - so the address is free and no trace of it
/// is left. Another customer's hashes are untouched.
#[test]
fn forgetting_the_customer_frees_the_address_and_removes_its_hash() {
    runtime().block_on(async {
        let Some(crm) = crm::<ErasedEmail>().await else {
            return;
        };
        let (c1, c2) = (id("c"), id("c"));
        assert_eq!(crm.register(&c1, "alice@example.com").await, "accepted");
        assert_eq!(crm.register(&c2, "bob@example.com").await, "accepted");
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "email_taken"
        );
        let before = crm.stored_hashes().await;
        assert_eq!(before.len(), 2);

        crm.forget(&c1).await;

        let after = crm.stored_hashes().await;
        assert_eq!(after.len(), 1);
        let erased = before.iter().find(|h| !after.contains(h)).unwrap();
        assert!(!crm.stores(erased).await);
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        assert_eq!(
            crm.register(&id("c"), "bob@example.com").await,
            "email_taken"
        );
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// `BlindErasure::Keep`: the customer is forgotten, its hash isn't, and
/// the address stays taken.
#[test]
fn a_kept_address_stays_taken_after_its_customer_is_forgotten() {
    runtime().block_on(async {
        let Some(crm) = crm::<KeptEmail>().await else {
            return;
        };
        let c1 = id("c");
        assert_eq!(crm.register(&c1, "alice@example.com").await, "accepted");
        crm.forget(&c1).await;
        assert_eq!(crm.stored_hashes().await.len(), 1);
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "email_taken"
        );
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// An instance that doesn't declare the `BlindTag` can't hash the tag, so
/// it refuses the command rather than store the address in the clear - a
/// refusal another instance can take over (`ANOTHER_INSTANCE_CODES`).
#[test]
fn an_instance_without_the_blind_tag_refuses_rather_than_store_the_value() {
    runtime().block_on(async {
        let Some(crm) = crm::<ErasedEmail>().await else {
            return;
        };
        let (database_url, _) = test_db().await.unwrap();
        // Declaring the types would need the `BlindTag` too: `build()`
        // refuses a blind mapping without one. So this instance declares
        // nothing, like one serving other bounded contexts.
        let (other, _) = small_builder(database_url).build().await.unwrap();
        let payload = serde_json::json!({ "customer_id": id("c"), "email": "carol@example.com" });
        let err = crm
            .submit_through(&other, "RegisterCustomer", payload)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "blind_tag_not_declared");
        assert!(err.another_instance_can_do_it());
        assert!(!crm.stores("carol").await);
        other.shutdown(std::time::Duration::from_secs(10)).await;
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// What `build()` refuses: a `BlindTag` without a `blind_index_key`, a
/// blind mapping no `BlindTag` declares, and a declared key mapped in the
/// clear.
#[test]
fn build_refuses_an_inconsistent_declaration() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc, subject) = bounded_context(&pool).await;

        let Err(no_key) = customer_types(&database_url, &bc, &subject)
            .blind_tag::<ErasedEmail>()
            .build()
            .await
        else {
            panic!("a BlindTag without a blind_index_key was accepted");
        };
        assert!(no_key.to_string().contains("blind_index_key"));

        let Err(undeclared) = customer_types(&database_url, &bc, &subject)
            .blind_index_key(blind_key())
            .build()
            .await
        else {
            panic!("a blind mapping without a BlindTag was accepted");
        };
        assert!(undeclared.to_string().contains("no BlindTag declares it"));

        struct PlainEmail;
        impl EventType for PlainEmail {
            type Payload = EmailPayload;
            const NAME: &'static str = "PlainEmail";
            fn tag_mappings() -> Vec<TagMapping> {
                vec![customer(), TagMapping::new("email", "email")]
            }
        }
        let Err(plain) = customer_types(&database_url, &bc, &subject)
            .event_type::<PlainEmail>()
            .blind_tag::<ErasedEmail>()
            .blind_index_key(blind_key())
            .build()
            .await
        else {
            panic!("a declared blind key mapped in the clear was accepted");
        };
        assert!(plain.to_string().contains("TagMapping::blind"));
    });
}

/// What startup refuses against what's stored: a normalizer version other
/// than the stored one, and making blind a key a registered type already
/// maps in the clear.
#[test]
fn startup_refuses_a_changed_normalizer_and_a_key_already_in_the_clear() {
    runtime().block_on(async {
        let Some(crm) = crm::<ErasedEmail>().await else {
            return;
        };
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
        let (database_url, pool) = test_db().await.unwrap();
        let subject = {
            let (_, subject) = bounded_context(&pool).await;
            subject
        };
        let Err(changed) = small_builder(database_url.clone())
            .bounded_context(crm.bc.clone())
            .blind_tag::<RenormalizedEmail>()
            .blind_index_key(blind_key())
            .reconciliation_role(subject)
            .build()
            .await
        else {
            panic!("a changed normalizer version was accepted");
        };
        assert_eq!(changed.code(), "blind_normalizer_changed");

        struct PlainEmail;
        impl EventType for PlainEmail {
            type Payload = EmailPayload;
            const NAME: &'static str = "PlainEmail";
            fn tag_mappings() -> Vec<TagMapping> {
                vec![customer(), TagMapping::new("email", "email")]
            }
        }
        let (bc, subject) = bounded_context(&pool).await;
        let (plain, _) = small_builder(database_url.clone())
            .bounded_context(bc.clone())
            .event_type::<PlainEmail>()
            .reconciliation_role(subject.clone())
            .build()
            .await
            .unwrap();
        plain.shutdown(std::time::Duration::from_secs(10)).await;
        let Err(in_use) = small_builder(database_url)
            .bounded_context(bc)
            .blind_tag::<ErasedEmail>()
            .blind_index_key(blind_key())
            .reconciliation_role(subject)
            .build()
            .await
        else {
            panic!("a key already mapped in the clear was made blind");
        };
        assert_eq!(in_use.code(), "blind_tag_key_in_use");
    });
}

// --- the key fingerprint (docs/architecture.md §208) ---

#[test]
fn startup_refuses_a_different_blind_index_key_and_records_a_missing_fingerprint() {
    runtime().block_on(async {
        let Some(crm) = crm::<ErasedEmail>().await else {
            return;
        };
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
        let (database_url, pool) = test_db().await.unwrap();
        let stored = || async {
            db::list_blind_tags(&pool, &crm.bc).await.unwrap()[0]
                .key_fingerprint
                .clone()
        };
        let recorded = stored().await;
        assert_eq!(recorded.as_ref().map(String::len), Some(64));

        // Not a reconciling instance: the check runs either way.
        let restart = |key: BlindIndexKey| {
            small_builder(database_url.clone())
                .bounded_context(crm.bc.clone())
                .event_type::<EmailClaimed>()
                .event_type::<EmailReleased>()
                .command_type::<RegisterCustomer>()
                .command_type::<ChangeEmail>()
                .encryption_master_key(master_key())
                .blind_tag::<ErasedEmail>()
                .blind_index_key(key)
                .build()
        };
        let Err(changed) = restart(BlindIndexKey::from_bytes([12; 32])).await else {
            panic!("a different blind_index_key was accepted");
        };
        assert_eq!(changed.code(), "blind_index_key_changed");
        assert_eq!(stored().await, recorded);

        let (same, _) = restart(blind_key()).await.unwrap();
        same.shutdown(std::time::Duration::from_secs(10)).await;

        // A row without one (a tenant whose creator couldn't compute it)
        // gets this process's, and is checked from then on.
        let schema = format!("\"bc_{}\"", crm.bc);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.blind_tags SET key_fingerprint = NULL"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let (backfilled, _) = restart(blind_key()).await.unwrap();
        backfilled
            .shutdown(std::time::Duration::from_secs(10))
            .await;
        assert_eq!(stored().await, recorded);
        let Err(changed) = restart(BlindIndexKey::from_bytes([12; 32])).await else {
            panic!("a different blind_index_key was accepted after the backfill");
        };
        assert_eq!(changed.code(), "blind_index_key_changed");
    });
}

// --- rotating the key (docs/architecture.md §209) ---

fn new_blind_key() -> BlindIndexKey {
    BlindIndexKey::from_bytes([13; 32])
}

/// `blind_tags`' row for `email`: its generation and key fingerprint.
async fn stored_generation(crm: &Crm) -> (u32, Option<String>) {
    let stored = db::list_blind_tags(&crm.pool, &crm.bc).await.unwrap();
    (stored[0].key_generation, stored[0].key_fingerprint.clone())
}

/// A reconciling instance configured with a new key and the old one as
/// previous rewraps every stored tag - events, their index, commands and
/// snapshot rows - without any address in the clear, so a forgotten
/// customer's kept address stays taken too. Nothing of the old tags is
/// left, every address taken before still is, and an instance with only
/// one of the two keys refuses to start.
#[test]
fn rotating_the_key_rewraps_every_stored_tag_and_keeps_every_value_taken() {
    runtime().block_on(async {
        let Some(crm) = crm::<KeptEmail>().await else {
            return;
        };
        let (forgotten, changed) = (id("c"), id("c"));
        assert_eq!(
            crm.register(&forgotten, "alice@example.com").await,
            "accepted"
        );
        assert_eq!(crm.register(&changed, "bob@example.com").await, "accepted");
        assert_eq!(
            crm.change(&changed, "bob@example.com", "robert@example.com")
                .await,
            "accepted"
        );
        crm.forget(&forgotten).await;
        let old_hashes = crm.stored_hashes().await;
        assert_eq!(old_hashes.len(), 3);
        let (generation, old_fingerprint) = stored_generation(&crm).await;
        assert_eq!(generation, 0);
        // A snapshot keyed by the tag holds a hash too.
        let schema = format!("\"bc_{}\"", crm.bc);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.snapshots (snapshot_name, tag_key, tag_value, \
             snapshot_version, as_of_sequence, state, updated_at) \
             VALUES ('Claims', 'email', $1, 1, 0, '{{}}', now())"
        )))
        .bind(&old_hashes[0])
        .execute(&crm.pool)
        .await
        .unwrap();

        let (database_url, _) = test_db().await.unwrap();
        let (rotated, report) = customer_types(&database_url, &crm.bc, &crm.subject)
            .blind_tag::<KeptEmail>()
            .blind_index_key(new_blind_key())
            .previous_blind_index_keys([blind_key()])
            .build()
            .await
            .unwrap();
        assert!(report.skipped_no_access.is_empty(), "{report:?}");
        let rotated = crm.served_by(rotated);
        let (generation, fingerprint) = stored_generation(&rotated).await;
        assert_eq!(generation, 1);
        assert_ne!(fingerprint, old_fingerprint);

        let new_hashes = rotated.stored_hashes().await;
        assert_eq!(new_hashes.len(), 3);
        for old in &old_hashes {
            assert!(!new_hashes.contains(old));
            assert!(!rotated.stores(old).await, "{old} left behind");
        }
        let (snapshot_tag,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT tag_value FROM {schema}.snapshots WHERE snapshot_name = 'Claims'"
        )))
        .fetch_one(&rotated.pool)
        .await
        .unwrap();
        assert!(new_hashes.contains(&snapshot_tag));

        // Every address keeps its state: the forgotten one stays taken, the
        // changed-from one is free, and new ones hash under the new key.
        assert_eq!(
            rotated.register(&id("c"), "Alice@Example.com").await,
            "email_taken"
        );
        assert_eq!(
            rotated.register(&id("c"), "robert@example.com").await,
            "email_taken"
        );
        assert_eq!(
            rotated.register(&id("c"), "bob@example.com").await,
            "accepted"
        );
        assert_eq!(
            rotated.register(&id("c"), "carol@example.com").await,
            "accepted"
        );
        assert_eq!(
            rotated.register(&id("c"), " carol@example.com").await,
            "email_taken"
        );
        rotated
            .skilj
            .shutdown(std::time::Duration::from_secs(10))
            .await;

        for (keys, what) in [
            (vec![blind_key()], "only the old key"),
            (vec![new_blind_key()], "only the new key"),
        ] {
            let mut builder = customer_instance(&database_url, &crm.bc).blind_tag::<KeptEmail>();
            for key in keys {
                builder = builder.blind_index_key(key);
            }
            let Err(refused) = builder.build().await else {
                panic!("an instance with {what} started");
            };
            assert_eq!(refused.code(), "blind_index_key_not_configured", "{what}");
        }
        let (again, _) = customer_instance(&database_url, &crm.bc)
            .blind_tag::<KeptEmail>()
            .blind_index_key(new_blind_key())
            .previous_blind_index_keys([blind_key()])
            .build()
            .await
            .unwrap();
        again.shutdown(std::time::Duration::from_secs(10)).await;
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// Instances already running when another rotates: one configured with
/// both keys notices under the write lock that its tags are of the old
/// generation and decides again, rehashed - so an address taken before
/// the rotation stays taken - and one with only the old key refuses with
/// a code another instance can take over.
#[test]
fn a_running_instance_rehashes_after_another_rotates_the_key() {
    runtime().block_on(async {
        let Some(crm) = crm::<ErasedEmail>().await else {
            return;
        };
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        let (database_url, _) = test_db().await.unwrap();
        let both_keys = || {
            customer_instance(&database_url, &crm.bc)
                .blind_tag::<ErasedEmail>()
                .blind_index_key(new_blind_key())
                .previous_blind_index_keys([blind_key()])
        };
        // Started before the rotation: hashes at generation 0.
        let (waiting, _) = both_keys().build().await.unwrap();
        let waiting = crm.served_by(waiting);
        assert_eq!(stored_generation(&crm).await.0, 0);
        assert_eq!(
            waiting.register(&id("c"), "alice@example.com").await,
            "email_taken"
        );

        let (rotating, _) = both_keys()
            .reconciliation_role(crm.subject.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(stored_generation(&crm).await.0, 1);

        assert_eq!(
            waiting.register(&id("c"), "ALICE@example.com").await,
            "email_taken"
        );
        assert_eq!(
            waiting.register(&id("c"), "dave@example.com").await,
            "accepted"
        );
        let rotating = crm.served_by(rotating);
        assert_eq!(
            rotating.register(&id("c"), " dave@example.com").await,
            "email_taken"
        );

        let refused = crm
            .submit_through(
                &crm.skilj,
                "RegisterCustomer",
                json!({ "customer_id": id("c"), "email": "erin@example.com" }),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code(), "blind_index_key_not_configured");
        assert!(skilj_retry::another_instance_can_do_it(refused.code()));

        for crm in [crm, waiting, rotating] {
            crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
        }
    });
}

// --- looking a blind tag up by its plaintext (docs/architecture.md §207) ---

const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDPHVFsUHiWXSbG
/TCig1cTQHNT6FnoYoZtMEjvDiQArsOL/dFoM9pmGRM9CfEtQGNum4TsimPtgJec
awfdPnW0uJCRlIF9wGmYdh2mYNBKw8jqxwp664Gd5uqH5L6A4pN8bfGO7+2niD6p
8t0cNeyYOd0PusbAEDcpzCUZmr6KQyM5i8/wk5oO98gntp+ZpMjUZabAD6R8DyhM
IZmV645jo5NPJG7zuSz+3dmKkNY0/GXz8YwvZ2swqmmOANRZHHfN1vgP2ycK02WZ
4yihx6EiuQCDseddBw+xit9KSvSq6GwmwnV1qVpMVNlSGGOeVX7v7JQ3z/BNbQ85
5p6s/FjhAgMBAAECggEAFu8fKghLIhNUjOpSbVxv0vDrFFqBQitOyV50ZQxCzlSL
0L+dZZWAVJfoOnUUYLdli0TrVioI4K7Bmw97AnO9IvLhB03TfPJGfxxtMhQ8XFsL
r3u03GGhq7N7OusIcUslm7ys5/AHd+qtTbJX65zJAx49LVW4VmI1SYqSfSBWgway
8uGYaXyCfwuxQ+xB4fQd6llm/+9dqS+U36LVSMWgEmVjceorYFhPVLfuX4A1wHjF
mDl40AwPBqzVbOIzFDMDikk4heFi6wlt6N3LGDtyBUUuzEg5TBhyiirvNvTjW+4V
Z4MZs3tez+IqM0+F4EsgAEQUU12YQxa4lobm8/zgZQKBgQD81FMzymNR6xWhUSwY
4RtkVntfMBOMp1rVGcVyBxOLKxEXF6ctk2rV38krfUI50h/lWzrbpl+zJvEe8D1H
vZjYj28sL3wf0CSnPYUeGANTxrW1dTiz1HVzzChfbAEWj3fsVrlghNcnHBkDDhqz
L/rPEfp//fB0SyLAEAJt87cgFwKBgQDRtjtH1gIkGn5GCS3u0FAbxV+qrUlTvu4t
Di1GcEw32jootQQSMZN1PxEvLuehaBlaASEL2OZzZlQ4q60LV1Jisvd7wqv5EYnG
o+sKtrCS5iXKfkxqTmg+JS7OZazggyvgBnv4GXT0US6/G4nw7C9JaS2jyOvPGIPS
K8dsWDIxxwKBgQCgr4FBxTticPqKUECqf0cdeilm0fNazXJZRcvLMNwm8vQlrQ6/
VJXt4BDG5xEUFovXBShfOVpRTkqo0x7fXYyq9l49wuAsh+kDsYHNIo3azMvny9yB
zmHnerWeD9KROBWLy4J96W+kl6L94hTuFWxd9psyhX4xKx+m2YXxw5d7eQKBgFB2
I86PHOkvRQ2oDfiX8nSFSQxaSk0Yb5fX3aUuBwBS+YeO1E4KuXH9zaEV1QeHwlpX
Ho/GG71hIKVRsSYtzc1Sr0PL0GHSydLuJ4tHxv3F0fAcf0M2bCaT656DQk4t5dKh
ikUJt2baEx59+XH3nLkE4t75gwhFdqZX5775I+EXAoGAfnpHlLZdGW48rl9Cl887
hRDjXDm/gP/ljCrvxxiWselEgaLj2o4NiT28QAfq7KgtOIpAeLAGzIBP6vkE7KFp
nAF+t4gRpooXXSI5oXCBcGI9a26q68UV3iDEmQGiP8kVHOsdzcOKY0qk1ulNAIV4
fU919gnTKorSq3FdV6zGZ8s=
-----END PRIVATE KEY-----
";

const TEST_MODULUS_N: &str = "zx1RbFB4ll0mxv0wooNXE0BzU-hZ6GKGbTBI7w4kAK7Di_3RaDPaZhkTPQnxLUBjbpuE7Ipj7YCXnGsH3T51tLiQkZSBfcBpmHYdpmDQSsPI6scKeuuBnebqh-S-gOKTfG3xju_tp4g-qfLdHDXsmDndD7rGwBA3KcwlGZq-ikMjOYvP8JOaDvfIJ7afmaTI1GWmwA-kfA8oTCGZleuOY6OTTyRu87ks_t3ZipDWNPxl8_GML2drMKppjgDUWRx3zdb4D9snCtNlmeMoocehIrkAg7HnXQcPsYrfSkr0quhsJsJ1dalaTFTZUhhjnlV-7-yUN8_wTW0POeaerPxY4Q";
const TEST_EXPONENT_E: &str = "AQAB";
const TEST_KID: &str = "test-key-1";
const TEST_ISSUER: &str = "https://idp.example.test/";
/// The `aud` this deployment's tokens carry - `IdpConfig` requires one.
const TEST_AUDIENCE: &str = "skilj-test-client";

async fn serve_jwks() -> String {
    let jwks = json!({
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": TEST_KID,
            "n": TEST_MODULUS_N,
            "e": TEST_EXPONENT_E,
        }]
    });
    let app = axum::Router::new().route(
        "/jwks.json",
        axum::routing::get(move || {
            let jwks = jwks.clone();
            async move { axum::Json(jwks) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind an ephemeral port for the JWKS test server");
    let addr = listener
        .local_addr()
        .expect("a bound listener always has a local address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("the JWKS test server stopped unexpectedly");
    });
    format!("http://{addr}/jwks.json")
}

fn sign_jwt(subject: &str) -> String {
    let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(TEST_KID.to_string());
    let claims = json!({
        "sub": subject,
        "iss": TEST_ISSUER,
        "aud": TEST_AUDIENCE,
        "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
    });
    let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes())
        .expect("the test private key PEM is well-formed");
    jsonwebtoken::encode(&header, &claims, &key).expect("signing a well-formed JWT never fails")
}

async fn graphql_request(
    router: &axum::Router,
    jwt: Option<&str>,
    query: &str,
    variables: serde_json::Value,
) -> serde_json::Value {
    let mut request = Request::builder().method("POST").uri("/graphql");
    if let Some(jwt) = jwt {
        request = request.header("authorization", format!("Bearer {jwt}"));
    }
    let request = request
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "query": query, "variables": variables }).to_string(),
        ))
        .unwrap();

    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

const CREATE_FROM_TEMPLATE: &str = "\
    mutation($template: String!, $name: String!, $roleId: ID!) { \
        createBoundedContextFromTemplate(template: $template, name: $name, roleId: $roleId, \
            level: ADMIN, canReadSensitive: false) { name } \
    }";

/// A tenant made from a bounded context with a blind tag records the key
/// fingerprint of its own constraint, not its template's, hashes the same
/// address differently, and is checked at startup like any other.
#[test]
fn a_tenant_records_its_own_key_fingerprint_and_is_checked_at_startup() {
    runtime().block_on(async {
        let jwks_url = serve_jwks().await;
        let idp = || {
            IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                TEST_AUDIENCE,
                SigningAlgorithm::Rs256,
            )
        };
        let Some(crm) = crm_with::<ErasedEmail>(|builder| builder.identity_provider(idp())).await
        else {
            return;
        };
        let superadmin = Role {
            id: generate_token_id(),
            external_subject: format!("superadmin_{}", generate_token_id()),
            name: "Superadmin".to_string(),
            superadmin: true,
            status: RoleStatus::Active,
            created_at: now(),
            revoked_at: None,
        };
        db::insert_role(&crm.pool, &superadmin).await.unwrap();
        let tenant = format!("tenant_{}", generate_token_id());
        let router = crm.skilj.graphql_router().await.unwrap();
        let created = graphql_request(
            &router,
            Some(&sign_jwt(&superadmin.external_subject)),
            CREATE_FROM_TEMPLATE,
            json!({ "template": crm.bc, "name": tenant, "roleId": superadmin.id }),
        )
        .await;
        assert!(created.get("errors").is_none(), "{created}");

        let fingerprint = |bc: String| {
            let pool = crm.pool.clone();
            async move {
                db::list_blind_tags(&pool, &bc).await.unwrap()[0]
                    .key_fingerprint
                    .clone()
            }
        };
        let own = skilj_core::blind::BlindIndex::new(
            blind_key(),
            [(tenant.clone(), "email".to_string(), normalize::email as _)],
        )
        .fingerprint(&tenant, "email", 0);
        assert!(own.is_some());
        assert_eq!(fingerprint(tenant.clone()).await, own);
        assert_ne!(fingerprint(crm.bc.clone()).await, own);

        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        let template_hashes = crm.stored_hashes().await;
        let template = crm.bc.clone();
        let in_tenant = Crm {
            bc: tenant.clone(),
            cache: EventCache::new(1000),
            ..crm
        };
        assert_eq!(
            in_tenant.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        assert_ne!(in_tenant.stored_hashes().await, template_hashes);
        in_tenant
            .skilj
            .shutdown(std::time::Duration::from_secs(10))
            .await;

        let schema = format!("\"bc_{tenant}\"");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.blind_tags SET key_fingerprint = repeat('0', 64)"
        )))
        .execute(&in_tenant.pool)
        .await
        .unwrap();
        let (database_url, _) = test_db().await.unwrap();
        let Err(changed) = small_builder(database_url.clone())
            .bounded_context(template)
            .blind_tag::<ErasedEmail>()
            .blind_index_key(blind_key())
            .build()
            .await
        else {
            panic!("a tenant's changed key fingerprint was accepted");
        };
        assert_eq!(changed.code(), "blind_index_key_changed");
        assert!(changed.to_string().contains(&tenant), "{changed}");
    });
}

/// A Role with admin access to `bc`, allowed to read sensitive fields or
/// not, and its signed JWT.
async fn reader(pool: &Pool, bc: &str, can_read_sensitive: bool) -> String {
    let role = Role {
        id: generate_token_id(),
        external_subject: format!("reader_{}", generate_token_id()),
        name: "Reader".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();
    let bounded_context = db::get_bounded_context(pool, bc).await.unwrap().unwrap();
    db::insert_role_access_mapping(
        pool,
        &RoleAccessMapping {
            role: role.clone(),
            bounded_context,
            level: AccessLevel::Admin,
            can_read_sensitive,
            scope: None,
            status: RoleStatus::Active,
            created_at: now(),
            revoked_at: None,
        },
    )
    .await
    .unwrap();
    sign_jwt(&role.external_subject)
}

const CLAIMS_OF: &str = "\
    query($bc: String!, $email: String!, $plaintext: Boolean) { \
        queryEvents(boundedContext: $bc, eventTypes: [\"EmailClaimed\", \"EmailReleased\"], \
            tags: [{ key: \"email\", value: $email, plaintext: $plaintext }]) { sequence } \
        countEvents(boundedContext: $bc, eventTypes: [\"EmailClaimed\"], \
            tags: [{ key: \"email\", value: $email, plaintext: $plaintext }]) \
    }";

/// Whether an address is taken can be asked before submitting the command
/// that would be rejected: `plaintext: true` has the server hash the value
/// as stored, whatever its case. Without the flag a value is taken as it
/// is, so a hash read back from an event's tags still finds it. A grant
/// that can't read sensitive fields is refused the plaintext lookup.
#[test]
fn a_blind_tag_can_be_looked_up_by_its_plaintext_with_a_sensitive_grant() {
    runtime().block_on(async {
        let jwks_url = serve_jwks().await;
        let Some(crm) = crm_with::<ErasedEmail>(|builder| {
            builder.identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                TEST_AUDIENCE,
                SigningAlgorithm::Rs256,
            ))
        })
        .await
        else {
            return;
        };
        assert_eq!(
            crm.register(&id("c"), "alice@example.com").await,
            "accepted"
        );
        let router = crm.skilj.graphql_router().await.unwrap();
        let sensitive = reader(&crm.pool, &crm.bc, true).await;
        let ask = |jwt: String, email: &str, plaintext: Option<bool>| {
            let router = router.clone();
            let variables = json!({ "bc": crm.bc, "email": email, "plaintext": plaintext });
            async move { graphql_request(&router, Some(&jwt), CLAIMS_OF, variables).await }
        };

        let taken = ask(sensitive.clone(), " Alice@Example.COM", Some(true)).await;
        assert_eq!(
            taken["data"]["queryEvents"].as_array().unwrap().len(),
            1,
            "{taken}"
        );
        assert_eq!(taken["data"]["countEvents"], 1, "{taken}");
        let free = ask(sensitive.clone(), "bob@example.com", Some(true)).await;
        assert_eq!(free["data"]["countEvents"], 0, "{free}");

        // Not hashed without the flag: the plaintext matches nothing, the
        // stored hash its event.
        let unflagged = ask(sensitive.clone(), "alice@example.com", None).await;
        assert_eq!(unflagged["data"]["countEvents"], 0, "{unflagged}");
        let hash = crm.stored_hashes().await.pop().unwrap();
        let by_hash = ask(sensitive, &hash, Some(false)).await;
        assert_eq!(by_hash["data"]["countEvents"], 1, "{by_hash}");

        let plain_reader = reader(&crm.pool, &crm.bc, false).await;
        let refused = ask(plain_reader, "alice@example.com", Some(true)).await;
        assert_eq!(
            refused["errors"][0]["extensions"]["code"], "blind_tag_lookup_not_granted",
            "{refused}"
        );
        crm.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}
