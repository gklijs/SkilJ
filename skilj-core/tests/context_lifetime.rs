//! Work prepared for one lifetime of a bounded context never lands in the
//! next one under the same name (docs/architecture.md §230, #221).
//!
//! Each writer resolves the encryption keys it needs before its own
//! transaction - an existing key is unwrapped at the key provider, a new
//! one wrapped there - and only then writes. A provider here holds that
//! call until the test has deleted the bounded context and created another
//! one with its name, with a data key of its own under the same row id.
//! Each writer must then refuse: storing would link the new context's
//! rows to the deleted context's keys.
//!
//! Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `submit_command.rs`.

use futures_util::StreamExt;
use jiff::Timestamp;
use skilj_core::access_control::{DirectCreationToken, ExternalEventToken, TokenStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::encryption::provider::BoxFuture;
use skilj_core::encryption::{self, InMemoryKeyProvider, KeyProvider, KeyService, WrappedDataKey};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, CommandType, Event, EventBroadcaster, EventType,
    MissedOccurrencePolicy,
};
use skilj_core::plugin::{CommandDispatcher, EventDispatcher, ProjectionDispatcher};
use skilj_core::shared::{generate_token_id, CommandDecision, SensitiveField};
use std::sync::Arc;
use std::time::Duration;

struct NoProjections;

impl ProjectionDispatcher for NoProjections {
    fn keys(&self, _: &str, _: &str, _: &Event) -> Option<Vec<String>> {
        None
    }
    fn project(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &Event,
        _: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }
    fn default_state(&self, _: &str, _: &str) -> Option<String> {
        None
    }
    fn owner_tag_key(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn requires_capability(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
}

/// Accepts every command with no events.
struct AcceptAll;

impl CommandDispatcher for AcceptAll {
    fn dispatch(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        Some(Ok(CommandDecision::Accepted { events: Vec::new() }))
    }
    fn required_role(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        Some(None)
    }
    fn snapshot_name(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        Some(None)
    }
    fn dispatch_from_snapshot(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &[Event],
        _: &skilj_core::plugin::DecisionContext,
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        unreachable!("no snapshots here")
    }
}

/// The payload of every scheduled occurrence.
struct Scheduled;

impl EventDispatcher for Scheduled {
    fn scheduled_payload(&self, _: &str, _: &str) -> Option<String> {
        Some(PAYLOAD.to_string())
    }
}

const PAYLOAD: &str = r#"{"customer_id":"c1","card":"4111"}"#;

/// A provider that holds its `unwrap` - or its `wrap`, with `on_wrap` -
/// until released, after saying on `entered` that it got there.
#[derive(Clone)]
struct Gated {
    inner: InMemoryKeyProvider,
    on_wrap: bool,
    entered: Arc<tokio::sync::Semaphore>,
    gate: Arc<tokio::sync::Semaphore>,
}

impl Gated {
    fn new(inner: InMemoryKeyProvider, on_wrap: bool) -> Self {
        Self {
            inner,
            on_wrap,
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }

    async fn hold(&self) {
        self.entered.add_permits(1);
        self.gate.acquire().await.unwrap().forget();
    }

    async fn reached(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.entered.acquire())
            .await
            .expect("the provider is called within 10 s")
            .unwrap()
            .forget();
    }
}

impl KeyProvider for Gated {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn wrap<'a>(
        &'a self,
        data_key: &'a [u8; 32],
    ) -> BoxFuture<'a, Result<WrappedDataKey, encryption::Error>> {
        Box::pin(async move {
            if self.on_wrap {
                self.hold().await;
            }
            self.inner.wrap(data_key).await
        })
    }
    fn unwrap<'a>(
        &'a self,
        wrapped: &'a WrappedDataKey,
    ) -> BoxFuture<'a, Result<[u8; 32], encryption::Error>> {
        Box::pin(async move {
            if !self.on_wrap {
                self.hold().await;
            }
            self.inner.unwrap(wrapped).await
        })
    }
    fn versions(&self) -> BoxFuture<'_, Result<encryption::KeyVersions, encryption::Error>> {
        self.inner.versions()
    }
    fn rotate(&self) -> BoxFuture<'_, Result<u32, encryption::Error>> {
        self.inner.rotate()
    }
    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), encryption::Error>> {
        self.inner.retire_below(version)
    }
    fn key(&self, key: &str) -> Result<Arc<dyn KeyProvider>, encryption::Error> {
        self.inner.key(key)
    }
    fn create(&self) -> BoxFuture<'_, Result<bool, encryption::Error>> {
        self.inner.create()
    }
    fn delete(&self) -> BoxFuture<'_, Result<(), encryption::Error>> {
        self.inner.delete()
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("a tokio runtime"))
}

static TEST_DB: tokio::sync::OnceCell<Option<Pool>> = tokio::sync::OnceCell::const_new();

async fn test_pool() -> Option<Pool> {
    TEST_DB
        .get_or_init(|| async {
            let url = skilj_test_support::database_url("skilj_context_lifetime_test").await?;
            let pool = match db::connect(&url).await {
                Ok(pool) => pool,
                Err(e) => {
                    eprintln!("skipping: connecting to the test database failed: {e}");
                    return None;
                }
            };
            if let Err(e) = db::migrate(&pool).await {
                eprintln!("skipping: migrating the test database failed: {e}");
                return None;
            }
            Some(pool)
        })
        .await
        .clone()
}

fn test_now() -> Timestamp {
    Timestamp::now()
        .round(
            jiff::TimestampRound::new()
                .smallest(jiff::Unit::Microsecond)
                .mode(jiff::RoundMode::Trunc),
        )
        .unwrap()
}

fn is_row_not_found(err: &skilj_core::Error) -> bool {
    matches!(err, skilj_core::Error::Database(sqlx::Error::RowNotFound))
}

/// `CardCharged`, its `card` encrypted under the `customer_id` subject's
/// key, creatable every way and scheduled every second from `position`.
fn card_charged(bc: &BoundedContext, position: Timestamp) -> EventType {
    EventType {
        bounded_context: bc.clone(),
        name: "CardCharged".to_string(),
        schema: r#"{"properties":{"customer_id":{"type":"string"},"card":{"type":"string"}}}"#
            .to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: vec![SensitiveField::new("card", "customer", "customer_id")],
        private_fields: Vec::new(),
        blob_fields: Vec::new(),
        external_creation_allowed: true,
        direct_creation_allowed: true,
        system_triggered_allowed: true,
        system_triggered_schedule: Some("* * * * * * *".to_string()),
        missed_occurrence_policy: Some(MissedOccurrencePolicy::ReplayBacklog),
        schedule_position: Some(position),
        last_fired_at: None,
        event_read_allowed: true,
    }
}

fn charge_card(bc: &BoundedContext) -> CommandType {
    CommandType {
        bounded_context: bc.clone(),
        name: "ChargeCard".to_string(),
        schema: r#"{"properties":{"customer_id":{"type":"string"},"card":{"type":"string"}}}"#
            .to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: vec![SensitiveField::new("card", "customer", "customer_id")],
        private_fields: Vec::new(),
        blob_fields: Vec::new(),
        rest_trigger_allowed: true,
    }
}

/// A bounded context with `CardCharged` and `ChargeCard`.
async fn seed(pool: &Pool, bc: &BoundedContext, position: Timestamp) -> (EventType, CommandType) {
    db::insert_bounded_context(pool, bc).await.unwrap();
    let et = card_charged(bc, position);
    db::upsert_event_type(pool, &et).await.unwrap();
    let ct = charge_card(bc);
    db::upsert_command_type(pool, &ct).await.unwrap();
    (et, ct)
}

/// Rows of `table` in `bc`'s schema.
async fn rows(pool: &Pool, bc: &str, table: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"bc_{bc}\".{table}"
    )))
    .fetch_one(pool)
    .await
    .unwrap()
}

/// One stale write: `c1`'s key exists in a fresh bounded context; `write`
/// starts with a key service whose unwrap is held, and once it is at the
/// provider the context is deleted and created again, the new one with a
/// key of its own at the same row id. Answers the bounded context, the
/// plain key service and what `write` answered.
async fn stale_write<T: Send + 'static>(
    pool: &Pool,
    gate_wrap: bool,
    write: impl FnOnce(Pool, BoundedContext, KeyService) -> BoxFuture<'static, T> + Send + 'static,
) -> (BoundedContext, KeyService, T) {
    let inner = InMemoryKeyProvider::new();
    let plain = KeyService::new(inner.clone());
    let gated = Gated::new(inner, gate_wrap);
    let bc = BoundedContext {
        name: format!("lifetime_{}", generate_token_id()),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    let position = Timestamp::from_second(test_now().as_second() - 10).unwrap();
    seed(pool, &bc, position).await;
    if !gate_wrap {
        db::get_or_create_encryption_key(pool, &bc.name, "customer", "c1", &plain)
            .await
            .unwrap();
    }
    let task = {
        let (pool, bc, keys) = (pool.clone(), bc.clone(), KeyService::new(gated.clone()));
        tokio::spawn(async move { write(pool, bc, keys).await })
    };
    gated.reached().await;
    if gate_wrap {
        // The writer holds its transaction: the deletion waits for it.
        let deletion = {
            let (pool, name) = (pool.clone(), bc.name.clone());
            tokio::spawn(async move { db::hard_delete_bounded_context(&pool, &name).await })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!deletion.is_finished(), "the deletion waits for the writer");
        gated.gate.add_permits(1);
        let answer = task.await.unwrap();
        deletion.await.unwrap().unwrap();
        seed(pool, &bc, position).await;
        return (bc, plain, answer);
    }
    db::hard_delete_bounded_context(pool, &bc.name)
        .await
        .unwrap();
    seed(pool, &bc, position).await;
    // The new context's first key: row id 1, as the deleted one's `c1`.
    db::get_or_create_encryption_key(pool, &bc.name, "customer", "other", &plain)
        .await
        .unwrap();
    gated.gate.add_permits(1);
    let answer = task.await.unwrap();
    (bc, plain, answer)
}

fn token(et: &EventType) -> ExternalEventToken {
    ExternalEventToken {
        id: generate_token_id(),
        secret: "s3cr3t".to_string(),
        status: TokenStatus::Active,
        created_at: test_now(),
        revoked_at: None,
        event_type: et.clone(),
        scope: None,
    }
}

#[test]
fn an_external_event_prepared_for_a_deleted_context_is_refused() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let (bc, _, answer) = stale_write(&pool, false, |pool, bc, keys| {
            Box::pin(async move {
                let et = card_charged(&bc, test_now());
                db::create_and_insert_external_event(
                    &pool,
                    &NoProjections,
                    &EventBroadcaster::new(16),
                    &EventCache::new(100),
                    &token(&et),
                    PAYLOAD.to_string(),
                    "kafka".to_string(),
                    None,
                    None,
                    None,
                    None,
                    test_now(),
                    Some(&keys),
                    &skilj_core::blind::BlindIndex::default(),
                )
                .await
                .map(|_| ())
            })
        })
        .await;
        let err = answer.unwrap_err();
        assert!(is_row_not_found(&err), "{err:?}");
        assert_eq!(rows(&pool, &bc.name, "events").await, 0);
    });
}

#[test]
fn an_external_event_batch_prepared_for_a_deleted_context_is_refused() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let (bc, _, answers) = stale_write(&pool, false, |pool, bc, keys| {
            Box::pin(async move {
                let et = card_charged(&bc, test_now());
                let item = db::ExternalEventBatchItem {
                    payload: PAYLOAD.to_string(),
                    source_content: "kafka".to_string(),
                    source_context: None,
                    correlation_id: None,
                    causation_id: None,
                    dedupe: None,
                };
                db::create_and_insert_external_events(
                    &pool,
                    &NoProjections,
                    &EventBroadcaster::new(16),
                    &EventCache::new(100),
                    &token(&et),
                    vec![item.clone(), item],
                    Some(&keys),
                    &skilj_core::blind::BlindIndex::default(),
                    false,
                )
                .await
                .into_iter()
                .map(|answer| answer.map(|_| ()))
                .collect::<Vec<_>>()
            })
        })
        .await;
        assert_eq!(answers.len(), 2);
        assert!(answers.iter().all(Result::is_err), "{answers:?}");
        assert_eq!(rows(&pool, &bc.name, "events").await, 0);
    });
}

#[test]
fn a_direct_event_prepared_for_a_deleted_context_is_refused() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let (bc, _, answer) = stale_write(&pool, false, |pool, bc, keys| {
            Box::pin(async move {
                let et = card_charged(&bc, test_now());
                db::create_and_insert_direct_event(
                    &pool,
                    &NoProjections,
                    &EventBroadcaster::new(16),
                    &EventCache::new(100),
                    &DirectCreationToken {
                        id: generate_token_id(),
                        secret: "s3cr3t".to_string(),
                        status: TokenStatus::Active,
                        created_at: test_now(),
                        revoked_at: None,
                        event_type: et,
                        scope: None,
                    },
                    PAYLOAD.to_string(),
                    None,
                    None,
                    test_now(),
                    Some(&keys),
                    &skilj_core::blind::BlindIndex::default(),
                )
                .await
                .map(|_| ())
            })
        })
        .await;
        let err = answer.unwrap_err();
        assert!(is_row_not_found(&err), "{err:?}");
        assert_eq!(rows(&pool, &bc.name, "events").await, 0);
    });
}

/// An accepted command with no events: only its own row would be written.
#[test]
fn a_command_with_no_events_prepared_for_a_deleted_context_is_refused() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let (bc, _, answer) = stale_write(&pool, false, |pool, bc, keys| {
            Box::pin(async move {
                let now = test_now();
                db::submit_command(
                    &pool,
                    &AcceptAll,
                    &NoProjections,
                    &EventBroadcaster::new(16),
                    &EventCache::new(100),
                    &charge_card(&bc),
                    PAYLOAD,
                    "client-1",
                    None,
                    None,
                    &[],
                    &[],
                    &[],
                    CommandDecision::Accepted { events: Vec::new() },
                    Some(&keys),
                    now,
                    None,
                    &[],
                    None,
                    None,
                    &skilj_core::plugin::DecisionContext::at(now),
                )
                .await
                .map(|outcome| matches!(outcome, SubmitCommandOutcome::Accepted { .. }))
            })
        })
        .await;
        let err = answer.unwrap_err();
        assert!(is_row_not_found(&err), "{err:?}");
        assert_eq!(rows(&pool, &bc.name, "commands").await, 0);
    });
}

#[test]
fn a_proposal_prepared_for_a_deleted_context_is_refused() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let (bc, _, answer) = stale_write(&pool, false, |pool, bc, keys| {
            Box::pin(async move {
                let ct = charge_card(&bc);
                db::propose_command(
                    &pool,
                    &ct,
                    &skilj_core::event_store::CommandAuthorised {
                        command_type: ct.clone(),
                        payload: PAYLOAD.to_string(),
                        client_id: "client-1".to_string(),
                        correlation_id: None,
                        causation_id: None,
                    },
                    &skilj_core::approvals::CommandApprovalPolicy::new(None, None),
                    None,
                    Some(&keys),
                    skilj_core::payload_limits::PayloadLimits::default(),
                    test_now(),
                )
                .await
                .map(|_| ())
            })
        })
        .await;
        let err = answer.unwrap_err();
        assert!(is_row_not_found(&err), "{err:?}");
        assert_eq!(rows(&pool, &bc.name, "command_proposals").await, 0);
    });
}

#[test]
fn an_encrypted_blob_prepared_for_a_deleted_context_is_refused_and_its_bytes_removed() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let (bc, _, answer) = stale_write(&pool, false, |pool, bc, keys| {
            Box::pin(async move {
                let store = db::PostgresBlobStore::new(pool.clone());
                let chunks = vec![Ok(b"a scanned card".to_vec())];
                db::upload_blob(
                    &pool,
                    &store,
                    &bc.name,
                    "text/plain",
                    futures_util::stream::iter(chunks).boxed(),
                    1024,
                    Some(db::BlobSubject {
                        subject_key: "customer",
                        subject_value: "c1",
                        key_service: &keys,
                    }),
                    test_now(),
                )
                .await
                .map(|_| ())
            })
        })
        .await;
        let err = answer.unwrap_err();
        assert!(is_row_not_found(&err), "{err:?}");
        assert_eq!(rows(&pool, &bc.name, "blobs").await, 0);
        assert_eq!(
            rows(&pool, &bc.name, "blob_bytes").await,
            0,
            "the bytes stored for the deleted context are removed"
        );
    });
}

/// The occurrence is not fired in the new context; the same occurrence,
/// fired for it afresh, is - so it was eligible there.
#[test]
fn a_scheduled_occurrence_prepared_for_a_deleted_context_is_not_fired() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let fire = |pool: Pool, bc: BoundedContext, keys: KeyService| async move {
            let et = db::get_event_type(&pool, &bc.name, "CardCharged")
                .await
                .unwrap()
                .unwrap();
            let position = et.schedule_position.unwrap();
            let at = position + jiff::SignedDuration::from_secs(1);
            db::fire_system_event(
                &pool,
                &NoProjections,
                &Scheduled,
                &EventBroadcaster::new(16),
                &EventCache::new(100),
                &bc.name,
                "CardCharged",
                at,
                test_now(),
                Some(&keys),
            )
            .await
            .map(|fired| fired.is_some())
        };
        let (bc, plain, answer) = stale_write(&pool, false, move |pool, bc, keys| {
            Box::pin(fire(pool, bc, keys))
        })
        .await;
        assert!(!answer.unwrap(), "not fired");
        assert_eq!(rows(&pool, &bc.name, "events").await, 0);
        assert!(
            fire(pool.clone(), bc.clone(), plain).await.unwrap(),
            "the occurrence is due in the new context"
        );
    });
}

/// A secret's key is wrapped inside its own transaction, which holds the
/// bounded context's row: the deletion waits for it, and the context
/// created afterwards has no secret.
#[test]
fn a_deletion_waits_for_a_secret_rotation_and_the_new_context_has_no_secret() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let (bc, _, answer) = stale_write(&pool, true, |pool, bc, keys| {
            Box::pin(async move {
                db::put_secret_value(
                    &pool,
                    &bc.name,
                    "card-api",
                    &skilj_core::secrets::SecretValue::new("t0ken"),
                    None,
                    &keys,
                    test_now(),
                )
                .await
                .map(|_| ())
            })
        })
        .await;
        answer.unwrap();
        assert_eq!(rows(&pool, &bc.name, "secret_versions").await, 0);
    });
}
