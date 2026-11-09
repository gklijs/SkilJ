//! Issue #174: locked validation experiments. No prepared evidence escapes
//! this test module; the production path still authenticates every leaf.
use super::*;
use crate::encryption::provider::BoxFuture;
use crate::encryption::{
    Error as KeyError, InMemoryKeyProvider, KeyProvider, KeyVersions, WrappedDataKey,
};
use crate::error::SkiljRejection;
use crate::plugin::{CommandDispatcher, DecisionContext};
use crate::shared::CommandDecision;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    Arc,
};
use std::time::{Duration, Instant};

type TestResult = crate::error::Result<super::super::commands::DecideOutcome>;

struct Provider {
    inner: InMemoryKeyProvider,
    fail: AtomicBool,
    delay: Duration,
    unwraps: AtomicUsize,
}
impl KeyProvider for Provider {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn wrap<'a>(&'a self, key: &'a [u8; 32]) -> BoxFuture<'a, Result<WrappedDataKey, KeyError>> {
        self.inner.wrap(key)
    }
    fn unwrap<'a>(
        &'a self,
        wrapped: &'a WrappedDataKey,
    ) -> BoxFuture<'a, Result<[u8; 32], KeyError>> {
        Box::pin(async move {
            self.unwraps.fetch_add(1, SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if self.fail.load(SeqCst) {
                return Err(KeyError::DecryptFailed);
            }
            self.inner.unwrap(wrapped).await
        })
    }
    fn versions(&self) -> BoxFuture<'_, Result<KeyVersions, KeyError>> {
        self.inner.versions()
    }
    fn rotate(&self) -> BoxFuture<'_, Result<u32, KeyError>> {
        self.inner.rotate()
    }
    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), KeyError>> {
        self.inner.retire_below(version)
    }
    fn key(&self, key: &str) -> Result<Arc<dyn KeyProvider>, KeyError> {
        self.inner.key(key)
    }
    fn create(&self) -> BoxFuture<'_, Result<bool, KeyError>> {
        self.inner.create()
    }
    fn delete(&self) -> BoxFuture<'_, Result<(), KeyError>> {
        self.inner.delete()
    }
}

struct Fixture {
    pool: Pool,
    bc: BoundedContext,
    provider: Arc<Provider>,
    service: KeyService,
    events: Vec<Event>,
    calls: AtomicUsize,
}
impl CommandDispatcher for Fixture {
    fn key_service(&self) -> Option<&KeyService> {
        Some(&self.service)
    }
    fn dispatch(
        &self,
        _: &str,
        _: &str,
        _: &str,
        events: &[Event],
    ) -> Option<crate::error::Result<CommandDecision>> {
        self.calls.fetch_add(1, SeqCst);
        for event in events
            .iter()
            .filter(|e| !e.event_type.sensitive_fields.is_empty())
        {
            let payload: serde_json::Value = serde_json::from_str(&event.payload).unwrap();
            assert_eq!(payload["secret"], "plaintext confined to decision");
        }
        Some(Ok(if events.len() > self.events.len() {
            CommandDecision::Rejected {
                reason: "probe".into(),
                kind: "probe".into(),
            }
        } else {
            CommandDecision::Accepted { events: vec![] }
        }))
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
        _: &DecisionContext,
    ) -> Option<crate::error::Result<CommandDecision>> {
        unreachable!()
    }
}

async fn pool() -> Option<Pool> {
    static POOL: tokio::sync::OnceCell<Option<Pool>> = tokio::sync::OnceCell::const_new();
    POOL.get_or_init(|| async {
        let url = skilj_test_support::database_url("skilj_decision_history_test").await?;
        let pool = connect(&url).await.unwrap();
        migrate(&pool).await.unwrap();
        Some(pool)
    })
    .await
    .clone()
}
impl Fixture {
    async fn new(pool: Pool, count: usize, sensitive_every: usize, delay: Duration) -> Self {
        let now = Utc::now();
        let bc = BoundedContext {
            name: format!("history_{}", crate::shared::generate_token_id()),
            status: BoundedContextStatus::Active,
            created_at: now,
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        insert_bounded_context(&pool, &bc).await.unwrap();
        let provider = Arc::new(Provider {
            inner: InMemoryKeyProvider::from_bytes([42; 32]),
            fail: AtomicBool::new(false),
            delay,
            unwraps: AtomicUsize::new(0),
        });
        let service = KeyService::from_arc(provider.clone());
        let (_, _, key) = get_or_create_encryption_key(&pool, &bc.name, "user", "alice", &service)
            .await
            .unwrap();
        let ciphertext = encryption::encrypt_leaf(&key, "plaintext confined to decision");
        let ty = EventType {
            bounded_context: bc.clone(),
            name: "Sensitive".into(),
            schema: "{}".into(),
            schema_version: 1,
            tag_mappings: vec![],
            owner_tag_key: None,
            sensitive_fields: vec![SensitiveField {
                field: "secret".into(),
                subject_key: "user".into(),
                subject_field: "user_id".into(),
            }],
            private_fields: vec![],
            blob_fields: vec![],
            external_creation_allowed: false,
            direct_creation_allowed: true,
            system_triggered_allowed: false,
            system_triggered_schedule: None,
            missed_occurrence_policy: None,
            schedule_position: None,
            last_fired_at: None,
            event_read_allowed: true,
        };
        upsert_event_type(&pool, &ty).await.unwrap();
        let plain = EventType {
            name: "Plain".into(),
            sensitive_fields: vec![],
            ..ty.clone()
        };
        upsert_event_type(&pool, &plain).await.unwrap();
        let events = (0..count).map(|i| Event {
            bounded_context: bc.clone(), event_type: if i % sensitive_every == 0 { ty.clone() } else { plain.clone() },
            payload: serde_json::json!({"user_id":"alice", "secret":ciphertext, "padding":"x".repeat(1024)}).to_string(),
            sequence: i as i64, tags: vec![Tag { key: "user".into(), value: Some("alice".into()) }],
            metadata: Metadata { r#type: "event".into(), version: 1, client_id: "probe".into(), created_at: now, correlation_id: None, causation_id: None },
            encryption_keys: vec![], origin: EventOrigin::DirectlyCreated,
        }).collect();
        Self {
            pool,
            bc,
            provider,
            service,
            events,
            calls: AtomicUsize::new(0),
        }
    }
    async fn prepare(&self) {
        let history =
            decision_history(&mut Db::Pool(&self.pool), &self.events, Some(&self.service))
                .await
                .unwrap();
        let decision = self
            .dispatch(&self.bc.name, "Probe", "{}", &history)
            .unwrap()
            .unwrap();
        assert!(matches!(decision, CommandDecision::Accepted { .. }));
    }
    async fn decide(&self, conn: &mut sqlx::PgConnection, extra: &[Event]) -> TestResult {
        self.decide_at(conn, extra, self.events.len() as i64 - 1, Some(&[]))
            .await
    }
    async fn decide_at(
        &self,
        conn: &mut sqlx::PgConnection,
        extra: &[Event],
        locked_highest: i64,
        batch_delta: Option<&[Event]>,
    ) -> TestResult {
        let ct = CommandType {
            bounded_context: self.bc.clone(),
            name: "Probe".into(),
            schema: "{}".into(),
            schema_version: 1,
            tag_mappings: vec![],
            owner_tag_key: None,
            sensitive_fields: vec![],
            private_fields: vec![],
            blob_fields: vec![],
            rest_trigger_allowed: false,
        };
        super::super::commands::decide_command_in_tx(
            conn,
            self,
            &ct,
            "{}",
            "probe",
            &self.events,
            &[Tag {
                key: "user".into(),
                value: Some("alice".into()),
            }],
            &self.events,
            CommandDecision::Accepted { events: vec![] },
            None,
            &[],
            None,
            locked_highest,
            extra,
            Default::default(),
            Some(self.events.len() as i64 - 1),
            None,
            batch_delta,
            None,
            &DecisionContext {
                now: Utc::now(),
                deadline: None,
            },
        )
        .await
    }
}

#[test]
fn erasure_after_preparation_refuses_even_with_warm_cached_key() {
    runtime().block_on(async {
        let Some(pool) = pool().await else { return };
        let f = Fixture::new(pool, 2, 2, Duration::ZERO).await;
        f.prepare().await;
        destroy_encryption_key(&f.pool, &f.bc.name, "user", "alice", Utc::now())
            .await
            .unwrap();
        let mut tx = f.pool.begin().await.unwrap();
        let error = f.decide(&mut tx, &[]).await.err().unwrap();
        assert_eq!(error.code(), "sensitive_history_unavailable");
        // Even reprovisioning the same subject cannot authenticate the old leaves.
        get_or_create_encryption_key(&f.pool, &f.bc.name, "user", "alice", &f.service)
            .await
            .unwrap();
        let error = f.decide(&mut tx, &[]).await.err().unwrap();
        assert_eq!(error.code(), "sensitive_history_unavailable");
        assert_eq!(f.calls.load(SeqCst), 1);
    });
}

#[test]
fn provider_failure_or_retirement_after_preparation_refuses_cold_unwrap() {
    runtime().block_on(async {
        let Some(pool) = pool().await else { return };
        for retirement in [false, true] {
            let f = Fixture::new(pool.clone(), 1, 1, Duration::ZERO).await;
            f.prepare().await;
            if retirement {
                f.provider.rotate().await.unwrap();
                f.provider.retire_below(2).await.unwrap();
            } else {
                f.provider.fail.store(true, SeqCst);
            }
            // Supported skilj retirement clears the cache; out-of-band provider
            // retirement alone does not promise immediate cache invalidation.
            f.service.clear_cache();
            let mut tx = f.pool.begin().await.unwrap();
            assert!(f.decide(&mut tx, &[]).await.is_err());
            assert_eq!(f.calls.load(SeqCst), 1);
        }
    });
}

#[test]
fn locked_validation_keeps_subject_row_locked_until_transaction_end() {
    runtime().block_on(async {
    let Some(pool) = pool().await else { return };
    let f = Fixture::new(pool, 1, 1, Duration::ZERO).await;
    f.prepare().await;
    let mut tx = f.pool.begin().await.unwrap();
    f.decide(&mut tx, &[]).await.unwrap();
    let mut eraser = f.pool.begin().await.unwrap();
    sqlx::query("SET LOCAL lock_timeout = '100ms'")
        .execute(&mut *eraser)
        .await
        .unwrap();
    let error = destroy_encryption_key_on(
        &mut *eraser,
        &schema_ident(&f.bc.name),
        "user",
        "alice",
        Utc::now(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, crate::error::Error::Database(ref e) if e.as_database_error().is_some_and(|e| e.code().as_deref() == Some("55P03")))
    );
    eraser.rollback().await.unwrap();
    tx.commit().await.unwrap();
    destroy_encryption_key(&f.pool, &f.bc.name, "user", "alice", Utc::now())
        .await
        .unwrap();
    });
}

#[test]
fn concurrent_append_redispatches_plaintext_but_returns_ciphertext_rejection_history() {
    runtime().block_on(async {
        let Some(pool) = pool().await else { return };
        let f = Fixture::new(pool, 1, 1, Duration::ZERO).await;
        f.prepare().await;
        // Same-transaction append is the extra_committed_events path used by batches.
        let mut extra = f.events[0].clone();
        extra.sequence = 1;
        let mut tx = f.pool.begin().await.unwrap();
        let result = f.decide(&mut tx, &[extra]).await.unwrap();
        let super::super::commands::DecideOutcome::Terminal(SubmitCommandOutcome::Rejected {
            matching_events,
            ..
        }) = result
        else {
            panic!("expected rejection")
        };
        assert_eq!(matching_events.len(), 2);
        assert!(matching_events
            .iter()
            .all(|e| !e.payload.contains("plaintext confined")));
        assert_eq!(f.calls.load(SeqCst), 2);
        assert!(!f.events[0].payload.contains("plaintext confined"));
    });
}

#[test]
fn changed_sensitive_registration_and_malformed_ciphertext_are_not_evidence() {
    runtime().block_on(async {
        let Some(pool) = pool().await else { return };
        let mut f = Fixture::new(pool, 1, 1, Duration::ZERO).await;
        f.prepare().await;
        // A freshly resolved history carries current registrations. The old
        // prepared history itself is not refreshed here: see the investigation.
        let mut changed = f.events[0].event_type.clone();
        changed.sensitive_fields[0].subject_key = "another_subject".into();
        upsert_event_type(&f.pool, &changed).await.unwrap();
        let mut tx = f.pool.begin().await.unwrap();
        // Existing prepared history retains the registration it was read with.
        // Record that limit explicitly; a future evidence API needs a generation.
        f.decide(&mut tx, &[]).await.unwrap();
        f.events[0].event_type = get_event_type(&f.pool, &f.bc.name, "Sensitive")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            f.decide(&mut tx, &[]).await.err().unwrap().code(),
            "sensitive_history_unavailable"
        );
        f.events[0].event_type.sensitive_fields[0].subject_key = "user".into();
        f.events[0].payload = r#"{"user_id":"alice","secret":"malformed"}"#.into();
        assert_eq!(
            f.decide(&mut tx, &[]).await.err().unwrap().code(),
            "sensitive_history_unavailable"
        );
    });
}

/// Test-only upper bound: a proof of prior authentication would have to bind
/// these subject identities to exact ciphertext, registrations and decision.
/// It still reads/locks the authoritative row and uses the real unwrap path.
async fn validation_prototype(f: &Fixture, conn: &mut sqlx::PgConnection) {
    let row: EncryptionKeyRow = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ENCRYPTION_KEY_COLUMNS} FROM {}.encryption_keys WHERE subject_key = 'user' AND subject_value = 'alice' AND status = 'active' FOR NO KEY UPDATE", schema_ident(&f.bc.name)
    ))).fetch_one(&mut *conn).await.unwrap();
    unwrap_row(&mut Db::Conn(conn), &f.bc.name, &f.service, &row)
        .await
        .unwrap();
}

#[test]
#[ignore = "explicit Postgres sensitive-history experiment"]
fn benchmark_locked_sensitive_history() {
    runtime().block_on(async {
        let pool = pool().await.expect("Postgres required for the experiment");
        println!(
            "events,sensitive_every,provider_delay_ms,cache,conflict,path,median_us,p95_us,unwraps"
        );
        for n in [100, 1000] {
            for sensitive_every in [1, 100] {
                for delay in [Duration::ZERO, Duration::from_millis(10)] {
                    let f = Fixture::new(pool.clone(), n, sensitive_every, delay).await;
                    f.prepare().await;
                    let mut extra = f.events[0].clone();
                    extra.sequence = n as i64;
                    for cold in [false, true] {
                        for conflict in [false, true] {
                            for prototype in [false, true] {
                                if prototype && conflict {
                                    continue;
                                }
                                let mut times = Vec::new();
                                let before = f.provider.unwraps.load(SeqCst);
                                for _ in 0..25 {
                                    if cold {
                                        f.service.clear_cache();
                                    }
                                    let mut tx = f.pool.begin().await.unwrap();
                                    super::super::commands::lock_bounded_context_sequence(
                                        &mut tx, &f.bc.name,
                                    )
                                    .await
                                    .unwrap();
                                    let started = Instant::now();
                                    if prototype {
                                        validation_prototype(&f, &mut tx).await;
                                    } else {
                                        f.decide(
                                            &mut tx,
                                            if conflict {
                                                std::slice::from_ref(&extra)
                                            } else {
                                                &[]
                                            },
                                        )
                                        .await
                                        .unwrap();
                                    }
                                    tx.rollback().await.unwrap();
                                    times.push(started.elapsed().as_micros());
                                }
                                times.sort_unstable();
                                println!(
                                    "{n},{sensitive_every},{},{},{conflict},{},{},{},{}",
                                    delay.as_millis(),
                                    if cold { "cold" } else { "warm" },
                                    if prototype {
                                        "key-only-prototype"
                                    } else {
                                        "production"
                                    },
                                    times[12],
                                    times[23],
                                    f.provider.unwraps.load(SeqCst) - before
                                );
                            }
                        }
                    }
                }
            }
        }
    });
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

#[test]
fn committed_append_after_preparation_is_read_and_redispatched() {
    runtime().block_on(async {
        let Some(pool) = pool().await else { return };
        let f = Fixture::new(pool, 1, 1, Duration::ZERO).await;
        assert_eq!(next_sequence(&f.pool, &f.bc.name).await.unwrap(), 0);
        insert_event(&f.pool, &f.events[0], None).await.unwrap();
        f.prepare().await;
        let mut appended = f.events[0].clone();
        appended.sequence = next_sequence(&f.pool, &f.bc.name).await.unwrap();
        insert_event(&f.pool, &appended, None).await.unwrap();
        let mut tx = f.pool.begin().await.unwrap();
        let highest = super::super::commands::lock_bounded_context_sequence(&mut tx, &f.bc.name)
            .await
            .unwrap();
        let outcome = f.decide_at(&mut tx, &[], highest, None).await.unwrap();
        let super::super::commands::DecideOutcome::Terminal(SubmitCommandOutcome::Rejected {
            matching_events,
            ..
        }) = outcome
        else {
            panic!("expected rejection")
        };
        assert_eq!(matching_events.len(), 2);
        assert!(matching_events
            .iter()
            .all(|e| !e.payload.contains("plaintext confined")));
        assert_eq!(f.calls.load(SeqCst), 2);
    });
}

#[test]
fn rewrap_after_preparation_uses_the_current_wrap() {
    runtime().block_on(async {
        let Some(pool) = pool().await else { return };
        let f = Fixture::new(pool, 1, 1, Duration::ZERO).await;
        f.prepare().await;
        let schema = schema_ident(&f.bc.name);
        let old: EncryptionKeyRow = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {ENCRYPTION_KEY_COLUMNS} FROM {schema}.encryption_keys"
        )))
        .fetch_one(&f.pool)
        .await
        .unwrap();
        f.provider.rotate().await.unwrap();
        let wrapped = WrappedDataKey {
            version: old.master_key_version.unwrap() as u32,
            ciphertext: old.wrapped_key.unwrap(),
        };
        let mut new = f.provider.rewrap(&[wrapped]).await.unwrap();
        let new = new.remove(0).unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.encryption_keys SET wrapped_key = $1, master_key_version = $2"
        )))
        .bind(new.ciphertext)
        .bind(new.version as i32)
        .execute(&f.pool)
        .await
        .unwrap();
        // Keep the old cached data key: the stored-wrap digest must miss it.
        let before = f.provider.unwraps.load(SeqCst);
        let mut tx = f.pool.begin().await.unwrap();
        f.decide(&mut tx, &[]).await.unwrap();
        assert_eq!(f.provider.unwraps.load(SeqCst), before + 1);
        assert_eq!(f.calls.load(SeqCst), 1);
    });
}

#[test]
fn mixed_history_preserves_unprotected_payload_bytes_and_source_ciphertext() {
    runtime().block_on(async {
        let Some(pool) = pool().await else { return };
        let mut f = Fixture::new(pool, 2, 2, Duration::ZERO).await;
        f.events[1].payload = "{ \"n\" : 1e2, \"ordered\" : true }\n".into();
        let original = f.events[0].payload.clone();
        let history = decision_history(&mut Db::Pool(&f.pool), &f.events, Some(&f.service))
            .await
            .unwrap();
        assert!(matches!(history, Cow::Owned(_)));
        assert_eq!(history[1].payload, f.events[1].payload);
        assert!(history[0].payload.contains("plaintext confined"));
        assert_eq!(f.events[0].payload, original);
        let plain = decision_history(&mut Db::Pool(&f.pool), &f.events[1..], None)
            .await
            .unwrap();
        assert!(matches!(plain, Cow::Borrowed(_)));
    });
}
