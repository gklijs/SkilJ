use super::super::provider::BoxFuture;
use super::super::{InMemoryKeyProvider, KeyVersions};
use super::*;
use futures_util::{future::join_all, poll};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
struct Provider {
    inner: InMemoryKeyProvider,
    calls: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
    wrap_gate: Arc<Semaphore>,
    failures: Arc<AtomicUsize>,
    delay: Duration,
}

impl Provider {
    fn new(blocked: bool) -> Self {
        Self {
            inner: InMemoryKeyProvider::from_bytes([7; 32]),
            calls: Arc::new(AtomicUsize::new(0)),
            gate: Arc::new(Semaphore::new(if blocked { 0 } else { 100_000 })),
            wrap_gate: Arc::new(Semaphore::new(100_000)),
            failures: Arc::new(AtomicUsize::new(0)),
            delay: Duration::ZERO,
        }
    }
}

impl KeyProvider for Provider {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn wrap<'a>(&'a self, data_key: &'a [u8; 32]) -> BoxFuture<'a, Result<WrappedDataKey, Error>> {
        Box::pin(async move {
            let wrapped = self.inner.wrap(data_key).await?;
            self.wrap_gate.acquire().await.unwrap().forget();
            Ok(wrapped)
        })
    }
    fn unwrap<'a>(&'a self, wrapped: &'a WrappedDataKey) -> BoxFuture<'a, Result<[u8; 32], Error>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // Capture the provider response before pausing: model a
            // response delayed in transit across retirement.
            let result = match self.failures.load(Ordering::SeqCst) {
                1 => Err(Error::ProviderUnavailable {
                    provider: self.name().into(),
                    message: "offline".into(),
                }),
                2 => Err(Error::ProviderRefused {
                    provider: self.name().into(),
                    message: "denied".into(),
                }),
                _ => self.inner.unwrap(wrapped).await,
            };
            self.gate.acquire().await.unwrap().forget();
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            result
        })
    }
    fn versions(&self) -> BoxFuture<'_, Result<KeyVersions, Error>> {
        self.inner.versions()
    }
    fn rotate(&self) -> BoxFuture<'_, Result<u32, Error>> {
        self.inner.rotate()
    }
    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), Error>> {
        self.inner.retire_below(version)
    }
    fn key(&self, key: &str) -> Result<Arc<dyn KeyProvider>, Error> {
        self.inner.key(key)
    }
    fn create(&self) -> BoxFuture<'_, Result<bool, Error>> {
        self.inner.create()
    }
    fn delete(&self) -> BoxFuture<'_, Result<(), Error>> {
        self.inner.delete()
    }
}

fn stored<'a>(provider: &'a str, wrapped: &'a WrappedDataKey) -> StoredWrap<'a> {
    StoredWrap {
        provider: Some(provider),
        version: Some(wrapped.version),
        ciphertext: &wrapped.ciphertext,
        nonce: None,
    }
}

fn cache(capacity: usize) -> Cache {
    Cache {
        entries: HashMap::new(),
        clock: VecDeque::new(),
        flights: HashMap::new(),
        generation: Arc::new(()),
        ttl: Duration::from_secs(300),
        capacity,
    }
}

#[test]
fn clock_evicts_one_entry_and_gives_hits_a_second_chance() {
    let mut cache = cache(3);
    for i in 0..3 {
        cache.insert([i; 32], DataKey([i; 32]));
    }
    assert_eq!(cache.get(&[0; 32]).unwrap().0, [0; 32]);
    cache.insert([3; 32], DataKey([3; 32]));
    assert_eq!(cache.entries.len(), 3);
    assert!(cache.get(&[1; 32]).is_none());
    for i in [0, 2, 3] {
        assert!(cache.get(&[i; 32]).is_some());
    }
    // An expired slot reused repeatedly does not grow clock metadata.
    for _ in 0..100 {
        cache.entries.get_mut(&[0; 32]).unwrap().at = Instant::now() - cache.ttl;
        assert!(cache.get(&[0; 32]).is_none());
        cache.insert([0; 32], DataKey([0; 32]));
    }
    assert_eq!(cache.clock.len(), 3);
    // All referenced: one full pass clears bits, then one eviction.
    for i in [0, 2, 3] {
        assert!(cache.get(&[i; 32]).is_some());
    }
    cache.insert([4; 32], DataKey([4; 32]));
    assert_eq!(cache.entries.len(), 3);
    assert_eq!(cache.clock.len(), 3);
}

#[test]
fn expiry_and_disabled_cache_never_return_keys() {
    let mut cache = cache(1);
    cache.insert([1; 32], DataKey([1; 32]));
    cache.entries.get_mut(&[1; 32]).unwrap().at = Instant::now() - cache.ttl;
    assert!(cache.get(&[1; 32]).is_none());
    assert!(cache.entries.get(&[1; 32]).unwrap().data_key.is_none());
    cache.insert([2; 32], DataKey([2; 32]));
    assert_eq!(cache.entries.len(), 1);
    assert!(!cache.entries.contains_key(&[1; 32]));
    for (ttl, capacity) in [(Duration::ZERO, 1), (Duration::from_secs(1), 0)] {
        cache.ttl = ttl;
        cache.capacity = capacity;
        cache.entries.clear();
        cache.clock.clear();
        cache.insert([1; 32], DataKey([1; 32]));
        assert!(cache.entries.is_empty());
        assert!(cache.clock.is_empty());
    }
}

#[tokio::test]
async fn identical_misses_share_success_and_failures_but_failures_are_retried() {
    for failure in 0..=2 {
        let provider = Provider::new(true);
        provider.failures.store(failure, Ordering::SeqCst);
        let wrapped = provider.wrap(&[9; 32]).await.unwrap();
        let service = KeyService::new(provider.clone());
        let mut requests: Vec<_> = (0..32)
            .map(|_| Box::pin(service.unwrap(stored(provider.name(), &wrapped))))
            .collect();
        for request in &mut requests {
            assert!(poll!(request).is_pending());
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        provider.gate.add_permits(1);
        for result in join_all(requests).await {
            match failure {
                0 => assert_eq!(result.unwrap().0, [9; 32]),
                1 => assert!(matches!(result, Err(Error::ProviderUnavailable { .. }))),
                _ => assert!(matches!(result, Err(Error::ProviderRefused { .. }))),
            }
        }
        provider.failures.store(0, Ordering::SeqCst);
        provider.gate.add_permits(1);
        assert_eq!(
            service
                .unwrap(stored(provider.name(), &wrapped))
                .await
                .unwrap()
                .0,
            [9; 32]
        );
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            if failure == 0 { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn cancellation_keeps_other_waiters_running_and_releases_abandoned_slots() {
    let provider = Provider::new(true);
    let wrapped = provider.wrap(&[9; 32]).await.unwrap();
    let service = KeyService::new(provider.clone());
    let mut first = Box::pin(service.unwrap(stored(provider.name(), &wrapped)));
    let mut second = Box::pin(service.unwrap(stored(provider.name(), &wrapped)));
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut second).is_pending());
    drop(first);
    provider.gate.add_permits(1);
    assert_eq!(second.await.unwrap().0, [9; 32]);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    service.clear_cache();
    let mut abandoned = Box::pin(service.unwrap(stored(provider.name(), &wrapped)));
    assert!(poll!(&mut abandoned).is_pending());
    drop(abandoned);
    assert_eq!(
        service.inner.unwrap_slots.available_permits(),
        MAX_UNWRAPS_IN_FLIGHT
    );
    provider.gate.add_permits(1);
    service
        .unwrap(stored(provider.name(), &wrapped))
        .await
        .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    assert!(service.inner.cache.lock().unwrap().flights.len() <= 1);
}

#[tokio::test]
async fn distinct_keys_run_independently_with_bounded_provider_work() {
    let provider = Provider::new(true);
    let service = KeyService::new(provider.clone());
    let mut wraps = Vec::new();
    for _ in 0..MAX_UNWRAPS_IN_FLIGHT + 1 {
        wraps.push(provider.wrap(&[9; 32]).await.unwrap());
    }
    let mut requests: Vec<_> = wraps
        .iter()
        .map(|wrapped| {
            Box::pin(tokio::task::unconstrained(
                service.unwrap(stored(provider.name(), wrapped)),
            ))
        })
        .collect();
    for request in &mut requests {
        assert!(poll!(request).is_pending());
    }
    assert_eq!(provider.calls.load(Ordering::SeqCst), MAX_UNWRAPS_IN_FLIGHT);
    assert_eq!(
        service.inner.cache.lock().unwrap().flights.len(),
        MAX_UNWRAPS_IN_FLIGHT
    );
    provider.gate.add_permits(MAX_UNWRAPS_IN_FLIGHT + 1);
    assert!(join_all(requests)
        .await
        .into_iter()
        .all(|result| result.is_ok()));
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        MAX_UNWRAPS_IN_FLIGHT + 1
    );
    assert_eq!(
        service.inner.unwrap_slots.available_permits(),
        MAX_UNWRAPS_IN_FLIGHT
    );
}

#[tokio::test]
async fn retirement_fences_old_responses_and_new_callers_do_not_join_them() {
    let provider = Provider::new(true);
    let wrapped = provider.wrap(&[9; 32]).await.unwrap();
    let service = KeyService::new(provider.clone());
    let mut old = Box::pin(service.unwrap(stored(provider.name(), &wrapped)));
    assert!(poll!(&mut old).is_pending());
    provider.rotate().await.unwrap();
    provider.retire_below(2).await.unwrap();
    service.clear_cache();
    let mut new = Box::pin(service.unwrap(stored(provider.name(), &wrapped)));
    assert!(poll!(&mut new).is_pending());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    provider.gate.add_permits(2);
    assert_eq!(old.await.unwrap().0, [9; 32]);
    assert!(matches!(
        new.await,
        Err(Error::KeyVersionRetired { version: 1 })
    ));
    assert!(service.inner.cache.lock().unwrap().entries.is_empty());
    provider.gate.add_permits(1);
    assert!(matches!(
        service.unwrap(stored(provider.name(), &wrapped)).await,
        Err(Error::KeyVersionRetired { .. })
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn digest_separates_provider_version_and_ciphertext_and_disabled_cache_stays_off() {
    let provider = Provider::new(false);
    let service = KeyService::new(provider.clone());
    let (_, wrapped) = service.generate().await.unwrap();
    assert!(matches!(
        service.unwrap(stored("unknown", &wrapped)).await,
        Err(Error::ProviderChanged { .. })
    ));
    let mut changed = wrapped.clone();
    changed.version += 1;
    assert_ne!(
        cache_key(provider.name(), &wrapped),
        cache_key(provider.name(), &changed)
    );
    changed = wrapped.clone();
    changed.ciphertext[0] ^= 1;
    assert_ne!(
        cache_key(provider.name(), &wrapped),
        cache_key(provider.name(), &changed)
    );
    assert_ne!(
        cache_key("another", &wrapped),
        cache_key(provider.name(), &wrapped)
    );
    assert!(service
        .unwrap(stored(provider.name(), &changed))
        .await
        .is_err());
    for (ttl, capacity) in [(Duration::ZERO, 5), (Duration::from_secs(300), 0)] {
        let service = KeyService::new(provider.clone()).with_cache(ttl, capacity);
        let before = provider.calls.load(Ordering::SeqCst);
        for _ in 0..2 {
            service
                .unwrap(stored(provider.name(), &wrapped))
                .await
                .unwrap();
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst) - before, 2);
    }
}

#[tokio::test]
async fn a_wrap_started_before_clear_cannot_repopulate_the_cache() {
    let provider = Provider::new(false);
    provider.wrap_gate.forget_permits(100_000);
    let service = KeyService::new(provider.clone());
    let mut generation = Box::pin(service.generate());
    assert!(poll!(&mut generation).is_pending());
    service.clear_cache();
    provider.wrap_gate.add_permits(1);
    let (_, wrapped) = generation.await.unwrap();
    assert!(service.inner.cache.lock().unwrap().entries.is_empty());
    service
        .unwrap(stored(provider.name(), &wrapped))
        .await
        .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalidation_while_waiting_for_admission_fences_the_eventual_response() {
    let provider = Provider::new(true);
    let service = KeyService::new(provider.clone());
    let mut wraps = Vec::new();
    for _ in 0..MAX_UNWRAPS_IN_FLIGHT + 1 {
        wraps.push(provider.wrap(&[9; 32]).await.unwrap());
    }
    let mut requests: Vec<_> = wraps
        .iter()
        .map(|wrapped| {
            Box::pin(tokio::task::unconstrained(
                service.unwrap(stored(provider.name(), wrapped)),
            ))
        })
        .collect();
    for request in &mut requests {
        assert!(poll!(request).is_pending());
    }
    service.clear_cache();
    provider.gate.add_permits(MAX_UNWRAPS_IN_FLIGHT + 1);
    assert!(join_all(requests)
        .await
        .into_iter()
        .all(|result| result.is_ok()));
    assert!(service.inner.cache.lock().unwrap().entries.is_empty());
    assert!(service.inner.cache.lock().unwrap().flights.is_empty());
}

// The pre-#175 service path, retained only for the comparison below.
type PreviousCache = Mutex<HashMap<Vec<u8>, (DataKey, Instant)>>;
async fn previous_unwrap(
    service: &KeyService,
    wrapped: &WrappedDataKey,
    entries: &PreviousCache,
    capacity: usize,
) -> DataKey {
    let provider = service.provider_named(service.provider_name()).unwrap();
    let wrapped = wrapped.clone();
    let digest = cache_key(provider.name(), &wrapped).to_vec();
    let cached = {
        let mut entries = entries.lock().unwrap();
        match entries.get(&digest) {
            Some((key, at)) if at.elapsed() < DEFAULT_DATA_KEY_CACHE_TTL => Some(key.clone()),
            Some(_) => {
                entries.remove(&digest);
                None
            }
            None => None,
        }
    };
    if let Some(key) = cached {
        return key;
    }
    let key = DataKey(provider.unwrap(&wrapped).await.unwrap());
    let mut entries = entries.lock().unwrap();
    if entries.len() >= capacity {
        entries.retain(|_, (_, at)| at.elapsed() < DEFAULT_DATA_KEY_CACHE_TTL);
        if entries.len() >= capacity {
            entries.clear();
        }
    }
    entries.insert(digest, (key.clone(), Instant::now()));
    key
}

#[tokio::test]
#[ignore = "cache latency and call-count experiment; run explicitly with --ignored --nocapture"]
async fn cache_workload_measurement() {
    const CAPACITY: usize = 128;
    const REQUESTS: usize = 2048;
    // Reproduce the previous policy alongside the production service.
    for working_set in [64, 128, 129, 256] {
        for policy in ["clear", "clock"] {
            let mut provider = Provider::new(false);
            provider.delay = Duration::from_millis(1);
            let service =
                KeyService::new(provider.clone()).with_cache(Duration::from_secs(300), CAPACITY);
            let mut wraps = Vec::new();
            for _ in 0..working_set {
                wraps.push(provider.wrap(&[9; 32]).await.unwrap());
            }
            let old_entries = PreviousCache::default();
            for wrapped in &wraps {
                if policy == "clock" {
                    service
                        .unwrap(stored(provider.name(), wrapped))
                        .await
                        .unwrap();
                } else {
                    previous_unwrap(&service, wrapped, &old_entries, CAPACITY).await;
                }
            }
            provider.calls.store(0, Ordering::SeqCst);
            let mut samples = Vec::new();
            // Stable pseudo-random reads rather than a cyclic scan, which
            // thrashes every replacement policy just above capacity.
            let mut state = 42u64;
            for _ in 0..REQUESTS {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let wrapped = &wraps[((state >> 32) as usize) % working_set];
                let at = Instant::now();
                if policy == "clock" {
                    service
                        .unwrap(stored(provider.name(), wrapped))
                        .await
                        .unwrap();
                } else {
                    previous_unwrap(&service, wrapped, &old_entries, CAPACITY).await;
                }
                samples.push(at.elapsed().as_nanos());
            }
            samples.sort_unstable();
            let calls = provider.calls.load(Ordering::SeqCst);
            let cache = service.inner.cache.lock().unwrap();
            // Shallow retained storage, excluding allocator/control bytes,
            // provider, caller futures, runtime, Arc and HashMap headers.
            let bytes = if policy == "clock" {
                cache.entries.capacity() * std::mem::size_of::<(Digest, Entry)>()
                    + cache.clock.capacity() * std::mem::size_of::<Digest>()
                    + cache.flights.capacity() * std::mem::size_of::<(Digest, Weak<Flight>)>()
            } else {
                let old_entries = old_entries.lock().unwrap();
                old_entries.capacity() * std::mem::size_of::<(Vec<u8>, (DataKey, Instant))>()
                    + old_entries.len() * 32
            };
            println!("policy={policy} capacity={CAPACITY} working_set={working_set} requests={REQUESTS} calls={calls} hit_pct={:.2} p50_ns={} p95_ns={} p99_ns={} shallow_bytes={bytes}",
                100.0 * (REQUESTS - calls) as f64 / REQUESTS as f64,
                samples[REQUESTS / 2], samples[REQUESTS * 95 / 100], samples[REQUESTS * 99 / 100]);
        }
    }
    for policy in ["scalar", "shared"] {
        let mut provider = Provider::new(false);
        provider.delay = Duration::from_millis(1);
        let service = KeyService::new(provider.clone());
        let wrapped = provider.wrap(&[9; 32]).await.unwrap();
        let old_entries = PreviousCache::default();
        let start = Instant::now();
        let samples = join_all((0..256).map(|_| async {
            let at = Instant::now();
            if policy == "shared" {
                service
                    .unwrap(stored(provider.name(), &wrapped))
                    .await
                    .unwrap();
            } else {
                previous_unwrap(
                    &service,
                    &wrapped,
                    &old_entries,
                    DEFAULT_DATA_KEY_CACHE_CAPACITY,
                )
                .await;
            }
            at.elapsed().as_nanos()
        }))
        .await;
        let elapsed = start.elapsed();
        let mut samples = samples;
        samples.sort_unstable();
        println!(
            "burst={policy} requests=256 calls={} elapsed_us={} p95_ns={} p99_ns={}",
            provider.calls.load(Ordering::SeqCst),
            elapsed.as_micros(),
            samples[256 * 95 / 100],
            samples[256 * 99 / 100]
        );
    }
}

#[tokio::test]
async fn a_waiter_that_stops_polling_does_not_keep_advertising_a_completed_failure() {
    let provider = Provider::new(true);
    provider.failures.store(1, Ordering::SeqCst);
    let wrapped = provider.wrap(&[9; 32]).await.unwrap();
    let service = KeyService::new(provider.clone());
    let mut first = Box::pin(service.unwrap(stored(provider.name(), &wrapped)));
    let mut stalled = Box::pin(service.unwrap(stored(provider.name(), &wrapped)));
    assert!(poll!(&mut first).is_pending());
    assert!(poll!(&mut stalled).is_pending());
    provider.gate.add_permits(1);
    assert!(matches!(
        first.await,
        Err(Error::ProviderUnavailable { .. })
    ));
    provider.failures.store(0, Ordering::SeqCst);
    provider.gate.add_permits(1);
    assert!(service
        .unwrap(stored(provider.name(), &wrapped))
        .await
        .is_ok());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert!(matches!(
        stalled.await,
        Err(Error::ProviderUnavailable { .. })
    ));
}

#[test]
fn a_default_capacity_insertion_preserves_the_rest_of_the_live_working_set() {
    let mut cache = cache(DEFAULT_DATA_KEY_CACHE_CAPACITY);
    for i in 0..=DEFAULT_DATA_KEY_CACHE_CAPACITY {
        let mut digest = [0; 32];
        digest[..8].copy_from_slice(&(i as u64).to_be_bytes());
        cache.insert(digest, DataKey([9; 32]));
    }
    assert_eq!(cache.entries.len(), DEFAULT_DATA_KEY_CACHE_CAPACITY);
    assert_eq!(cache.clock.len(), DEFAULT_DATA_KEY_CACHE_CAPACITY);
    assert!(!cache.entries.contains_key(&[0; 32]));
}

#[tokio::test]
async fn a_batch_asks_each_key_once_and_answers_in_order_without_filling_the_cache() {
    let provider = Provider::new(false);
    let legacy = EncryptionMasterKey::from_bytes([3; 32]);
    let service = KeyService::new(provider.clone()).with_legacy_master_key(legacy.clone());
    let name = provider.name().to_string();
    let other = service.provider_for(Some("other")).unwrap();
    other.create().await.unwrap();
    let wrapped = [
        provider.wrap(&[1; 32]).await.unwrap(),
        provider.wrap(&[2; 32]).await.unwrap(),
    ];
    let elsewhere = other.wrap(&[4; 32]).await.unwrap();
    let corrupt = WrappedDataKey {
        version: 1,
        ciphertext: vec![0, 1, 2],
    };
    let (_, old_wrap, old_nonce) = super::super::generate_and_wrap_data_key(&legacy);
    // One already in the cache: answered from it, not asked for.
    service.unwrap(stored(&name, &wrapped[1])).await.unwrap();
    let asked = provider.calls.load(Ordering::SeqCst);

    let batch = [
        stored(&name, &wrapped[0]),
        StoredWrap {
            provider: None,
            version: None,
            ciphertext: &old_wrap,
            nonce: Some(&old_nonce),
        },
        stored("in-memory:unknown", &wrapped[0]),
        stored(&name, &corrupt),
        stored(other.name(), &elsewhere),
        stored(&name, &wrapped[1]),
    ];
    let results = service.unwrap_batch(&batch).await;
    assert_eq!(results.len(), 6);
    assert_eq!(results[0].as_ref().ok().unwrap().0, [1; 32]);
    assert!(results[1].is_ok());
    assert!(matches!(results[2], Err(Error::ProviderChanged { .. })));
    assert!(matches!(results[3], Err(Error::DecryptFailed)));
    assert_eq!(results[4].as_ref().ok().unwrap().0, [4; 32]);
    assert_eq!(results[5].as_ref().ok().unwrap().0, [2; 32]);
    assert_eq!(provider.calls.load(Ordering::SeqCst), asked + 2);

    // Nothing the batch read was kept: the same batch asks again.
    service.unwrap_batch(&batch[..1]).await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), asked + 3);

    // A key that can't answer fails its own items, with its error, and
    // neither the other key's nor the legacy ones.
    provider.failures.store(1, Ordering::SeqCst);
    let results = service.unwrap_batch(&batch).await;
    assert!(matches!(results[0], Err(Error::ProviderUnavailable { .. })));
    assert!(results[1].is_ok());
    assert!(matches!(results[3], Err(Error::ProviderUnavailable { .. })));
    assert!(results[4].is_ok());
    assert!(results[5].is_ok(), "from the cache");

    // Without the legacy key a legacy one says so.
    let plain = KeyService::new(provider.clone());
    let results = plain.unwrap_batch(&batch[1..2]).await;
    assert!(matches!(results[0], Err(Error::LegacyMasterKeyRequired)));
}
