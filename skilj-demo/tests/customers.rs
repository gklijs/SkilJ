//! A bounded context generated from `tests/customers.skilj.toml`
//! (`build.rs`), with a blind tag (docs/architecture.md §205): a
//! customer's email address is unique, stored only encrypted, and freed
//! when the customer is forgotten. What's hand-written here is what is
//! always hand-written: the `decide_*` functions. The rest is proof that
//! the generated `BlindTag`, blind tag mappings and sensitive fields
//! compile and work against a real Postgres.

use skilj::{BlindIndexKey, EncryptionMasterKey, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, EventBroadcaster};
use skilj_core::shared::{generate_token_id, CommandDecision, EventSpec};

include!(concat!(env!("OUT_DIR"), "/customers_generated.rs"));

/// The query selected the address's latest claim or release; it's taken
/// when that is a claim. The events' addresses are ciphertext here - the
/// tag already matched the right ones.
fn taken(events: &[CustomersEvent]) -> bool {
    matches!(events.last(), Some(CustomersEvent::EmailClaimed(_)))
}

fn email_taken() -> CommandDecision {
    CommandDecision::Rejected {
        reason: "this email address is taken".into(),
        kind: "email_taken".into(),
    }
}

fn decide_register_customer(
    payload: &RegisterCustomerPayload,
    events: &[CustomersEvent],
) -> CommandDecision {
    if taken(events) {
        return email_taken();
    }
    CommandDecision::Accepted {
        events: vec![EventSpec {
            event_type: "EmailClaimed".into(),
            payload: serde_json::json!({
                "customer_id": payload.customer_id,
                "email": payload.email,
            }),
        }],
    }
}

fn decide_change_email(payload: &ChangeEmailPayload, events: &[CustomersEvent]) -> CommandDecision {
    if taken(events) {
        return email_taken();
    }
    CommandDecision::Accepted {
        events: vec![
            EventSpec {
                event_type: "EmailReleased".into(),
                payload: serde_json::json!({
                    "customer_id": payload.customer_id,
                    "email": payload.old_email,
                }),
            },
            EventSpec {
                event_type: "EmailClaimed".into(),
                payload: serde_json::json!({
                    "customer_id": payload.customer_id,
                    "email": payload.email,
                }),
            },
        ],
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
    EncryptionMasterKey::from_bytes([5; 32])
}

struct Customers {
    skilj: Skilj,
    pool: Pool,
    cache: EventCache,
}

static TEST_DB: tokio::sync::OnceCell<Option<(String, Pool)>> = tokio::sync::OnceCell::const_new();

/// `BOUNDED_CONTEXT` is fixed by the `.skilj.toml`, so every test shares
/// the one bounded context, made once; each test uses addresses of its
/// own.
async fn customers() -> Option<Customers> {
    let (database_url, pool) = TEST_DB
        .get_or_init(|| async {
            let url = skilj_test_support::database_url("skilj_demo_customers_test").await?;
            let pool = db::connect(&url).await.unwrap();
            db::migrate(&pool).await.unwrap();
            db::insert_bounded_context(
                &pool,
                &BoundedContext {
                    name: BOUNDED_CONTEXT.to_string(),
                    status: BoundedContextStatus::Active,
                    created_at: now(),
                    created_by: ContextCreator::SystemCreator,
                    template: None,
                },
            )
            .await
            .unwrap();
            Some((url, pool))
        })
        .await
        .clone()?;
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
    let bounded_context = db::get_bounded_context(&pool, BOUNDED_CONTEXT)
        .await
        .unwrap()
        .unwrap();
    db::insert_role_access_mapping(
        &pool,
        &RoleAccessMapping {
            role: role.clone(),
            bounded_context,
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
    // `auto_register` picks up the generated types (and the library's
    // banking/courses ones, whose bounded contexts this role can't reach,
    // so reconciliation skips them).
    let (skilj, _) = Skilj::builder(database_url)
        .auto_register()
        .encryption_master_key(master_key())
        .blind_index_key(BlindIndexKey::from_bytes([9; 32]))
        .reconciliation_role(role.external_subject)
        .build()
        .await
        .unwrap();
    Some(Customers {
        skilj,
        pool,
        cache: EventCache::new(1000),
    })
}

impl Customers {
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
            SubmitCommandOutcome::Deduplicated { .. } => "deduplicated".into(),
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn register(&self, customer_id: &str, email: &str) -> String {
        self.submit(
            "RegisterCustomer",
            serde_json::json!({ "customer_id": customer_id, "email": email }),
        )
        .await
    }

    /// Whether `needle` is stored anywhere in the bounded context's events
    /// or commands - tags, queries or payloads.
    async fn stores(&self, needle: &str) -> bool {
        let (found,): (bool,) = sqlx::query_as(
            "SELECT EXISTS (SELECT 1 FROM bc_customers.events \
                WHERE tags::text LIKE $1 OR payload LIKE $1) \
             OR EXISTS (SELECT 1 FROM bc_customers.commands \
                WHERE consistency_tags::text LIKE $1 OR consistency_query::text LIKE $1 \
                OR payload LIKE $1)",
        )
        .bind(format!("%{needle}%"))
        .fetch_one(&self.pool)
        .await
        .unwrap();
        found
    }
}

fn id(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

/// An address is taken once, however it's cased, and the address is
/// stored nowhere in the clear.
#[test]
fn an_address_is_registered_once_and_never_stored_in_the_clear() {
    runtime().block_on(async {
        let Some(c) = customers().await else {
            return;
        };
        let local = id("ada");
        let address = format!("{local}@example.com");
        assert_eq!(c.register(&id("c"), &address).await, "accepted");
        assert_eq!(
            c.register(&id("c"), &format!(" {}", address.to_uppercase()))
                .await,
            "email_taken"
        );
        assert!(!c.stores(&local).await);
        c.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}

/// Changing an address frees the old one; forgetting the customer frees
/// the new one (the generated `BlindErasure::with_subject("customer")`).
#[test]
fn a_changed_or_forgotten_address_is_free_again() {
    runtime().block_on(async {
        let Some(c) = customers().await else {
            return;
        };
        let (old, new) = (
            format!("{}@example.com", id("old")),
            format!("{}@example.com", id("new")),
        );
        let customer = id("c");
        assert_eq!(c.register(&customer, &old).await, "accepted");
        let change = serde_json::json!({ "customer_id": customer, "old_email": old, "email": new });
        assert_eq!(c.submit("ChangeEmail", change).await, "accepted");
        assert_eq!(c.register(&id("c"), &new).await, "email_taken");
        assert_eq!(c.register(&id("c"), &old).await, "accepted");

        db::forget_subject_in_blind_tags(&c.pool, BOUNDED_CONTEXT, "customer", &customer)
            .await
            .unwrap();
        db::destroy_encryption_key(&c.pool, BOUNDED_CONTEXT, "customer", &customer, now())
            .await
            .unwrap();
        assert_eq!(c.register(&id("c"), &new).await, "accepted");
        c.skilj.shutdown(std::time::Duration::from_secs(10)).await;
    });
}
