//! [`KeyService`]: the handle every path that seals or opens a sensitive
//! field goes through (docs/architecture.md §230) - the configured
//! [`KeyProvider`], a short-lived cache of unwrapped data keys, and, while
//! a deployment still has them, the raw master key that wrapped data keys
//! before key providers existed.

use super::provider::{KeyProvider, WrappedDataKey};
use super::{DataKey, EncryptionMasterKey, Error};
use futures_util::future::{FutureExt, Shared};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// How a data key is stored: by the provider it names, under a version, or,
/// with `provider: None`, wrapped by the legacy raw master key with its own
/// nonce - the format from before key providers.
#[derive(Debug, Clone, Copy)]
pub struct StoredWrap<'a> {
    /// The provider that wrapped it, `None` for a legacy row.
    pub provider: Option<&'a str>,
    /// The master key version, `None` for a legacy row.
    pub version: Option<u32>,
    /// The wrapped key.
    pub ciphertext: &'a [u8],
    /// A legacy row's nonce.
    pub nonce: Option<&'a [u8]>,
}

/// The default time an unwrapped data key is kept in memory.
pub const DEFAULT_DATA_KEY_CACHE_TTL: Duration = Duration::from_secs(300);
/// The default number of unwrapped data keys kept in memory.
pub const DEFAULT_DATA_KEY_CACHE_CAPACITY: usize = 10_000;

// A clock hit sets one bit instead of relinking an LRU list. Eviction
// gives recently read entries a second chance; expired entries need none.
struct Entry {
    data_key: Option<DataKey>,
    at: Instant,
    referenced: bool,
}

type Digest = [u8; 32];
type Flight = Shared<super::provider::BoxFuture<'static, Result<DataKey, Error>>>;
const MAX_UNWRAPS_IN_FLIGHT: usize = 64;

struct Cache {
    entries: HashMap<Digest, Entry>,
    clock: VecDeque<Digest>,
    flights: HashMap<Digest, Weak<Flight>>,
    // Identity rather than a wrapping counter: an old operation can never
    // acquire the identity of a later invalidation.
    generation: Arc<()>,
    ttl: Duration,
    capacity: usize,
}

impl Cache {
    fn get(&mut self, key: &Digest) -> Option<DataKey> {
        let entry = self.entries.get_mut(key)?;
        if entry.at.elapsed() < self.ttl {
            entry.referenced = true;
            entry.data_key.clone()
        } else {
            // Forget the expired key now, retaining only its clock slot
            // until insertion reuses it. Removing just the map entry
            // would accumulate duplicate clock slots on repeated expiry.
            entry.data_key = None;
            None
        }
    }

    fn insert(&mut self, key: Digest, data_key: DataKey) {
        if self.capacity == 0 || self.ttl.is_zero() {
            return;
        }
        if !self.entries.contains_key(&key) {
            while self.entries.len() >= self.capacity {
                let Some(candidate) = self.clock.pop_front() else {
                    break;
                };
                if let Some(entry) = self.entries.get_mut(&candidate) {
                    if entry.at.elapsed() < self.ttl && entry.referenced {
                        entry.referenced = false;
                        self.clock.push_back(candidate);
                    } else {
                        self.entries.remove(&candidate);
                    }
                }
            }
            self.clock.push_back(key);
        }
        self.entries.insert(
            key,
            Entry {
                data_key: Some(data_key),
                at: Instant::now(),
                referenced: false,
            },
        );
    }

    fn flight(&self, key: &Digest) -> Option<Arc<Flight>> {
        self.flights.get(key).and_then(Weak::upgrade)
    }
}

struct Inner {
    provider: Arc<dyn KeyProvider>,
    /// Every key this process has used, by name - the default one and the
    /// bounded contexts' own (docs/architecture.md §230) - so a stored
    /// data key finds the key that wrapped it.
    providers: Mutex<HashMap<String, Arc<dyn KeyProvider>>>,
    legacy: Option<EncryptionMasterKey>,
    cache: Mutex<Cache>,
    unwrap_slots: Arc<Semaphore>,
}

/// The configured [`KeyProvider`] with what skilj keeps around it (see the
/// module doc). Cheap to clone: clones share the provider and the cache.
#[derive(Clone)]
pub struct KeyService {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for KeyService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyService")
            .field("provider", &self.inner.provider.name())
            .field("legacy", &self.inner.legacy.is_some())
            .finish()
    }
}

impl KeyService {
    /// `provider` with the default cache.
    pub fn new(provider: impl KeyProvider) -> Self {
        Self::from_arc(Arc::new(provider))
    }

    /// [`new`](Self::new) for a provider already shared.
    pub fn from_arc(provider: Arc<dyn KeyProvider>) -> Self {
        Self::build(
            provider,
            None,
            DEFAULT_DATA_KEY_CACHE_TTL,
            DEFAULT_DATA_KEY_CACHE_CAPACITY,
        )
    }

    fn build(
        provider: Arc<dyn KeyProvider>,
        legacy: Option<EncryptionMasterKey>,
        ttl: Duration,
        capacity: usize,
    ) -> Self {
        // Only called before the service is shared (construction and the
        // builder-time `with_*`), so nothing remembered is lost.
        let providers = HashMap::from([(provider.name().to_string(), Arc::clone(&provider))]);
        Self {
            inner: Arc::new(Inner {
                provider,
                providers: Mutex::new(providers),
                legacy,
                cache: Mutex::new(Cache {
                    entries: HashMap::new(),
                    clock: VecDeque::new(),
                    flights: HashMap::new(),
                    generation: Arc::new(()),
                    ttl,
                    capacity,
                }),
                unwrap_slots: Arc::new(Semaphore::new(MAX_UNWRAPS_IN_FLIGHT)),
            }),
        }
    }

    /// This service, also able to unwrap data keys stored by a version of
    /// skilj before key providers, under `master_key`
    /// (docs/architecture.md §230) - until they are rewrapped by the
    /// provider.
    pub fn with_legacy_master_key(self, master_key: EncryptionMasterKey) -> Self {
        let cache = self.cache_settings();
        Self::build(
            Arc::clone(&self.inner.provider),
            Some(master_key),
            cache.0,
            cache.1,
        )
    }

    /// This service with its unwrapped data keys kept for `ttl`, at most
    /// `capacity` of them; zero capacity or TTL disables retention. Clock
    /// eviction gives recently read keys a second chance, evicting one
    /// key at a time. TTL is measured from insertion, not the last hit.
    /// Concurrent misses for the same stored wrap share one provider
    /// request, including its error; failures are not cached. At most 64
    /// provider unwraps run concurrently per service, even with retention
    /// disabled. Cancelling every waiter cancels its provider request.
    pub fn with_cache(self, ttl: Duration, capacity: usize) -> Self {
        Self::build(
            Arc::clone(&self.inner.provider),
            self.inner.legacy.clone(),
            ttl,
            capacity,
        )
    }

    fn cache_settings(&self) -> (Duration, usize) {
        let cache = self.inner.cache.lock().unwrap_or_else(|e| e.into_inner());
        (cache.ttl, cache.capacity)
    }

    /// The default key's provider: the key every bounded context without
    /// one of its own uses.
    pub fn provider(&self) -> &dyn KeyProvider {
        self.inner.provider.as_ref()
    }

    /// The default key's provider, shared.
    pub fn default_provider(&self) -> Arc<dyn KeyProvider> {
        Arc::clone(&self.inner.provider)
    }

    /// The provider of the key named `key` (docs/architecture.md §230), or
    /// of the default key for `None`; remembered, so data keys it wrapped
    /// are found by [`unwrap`](Self::unwrap) from then on.
    pub fn provider_for(&self, key: Option<&str>) -> Result<Arc<dyn KeyProvider>, Error> {
        let Some(key) = key else {
            return Ok(self.default_provider());
        };
        let provider = self.inner.provider.key(key)?;
        let mut providers = self
            .inner
            .providers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Ok(Arc::clone(
            providers
                .entry(provider.name().to_string())
                .or_insert(provider),
        ))
    }

    /// A key already used by this process, by its provider name.
    pub fn provider_named(&self, name: &str) -> Option<Arc<dyn KeyProvider>> {
        self.inner
            .providers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
    }

    /// The provider's name, as stored next to each data key it wraps.
    pub fn provider_name(&self) -> &str {
        self.inner.provider.name()
    }

    /// Whether a legacy master key is configured.
    pub fn has_legacy_master_key(&self) -> bool {
        self.inner.legacy.is_some()
    }

    /// A fresh data key, and its wrapped form to store, by the default key.
    pub async fn generate(&self) -> Result<(DataKey, WrappedDataKey), Error> {
        self.generate_with(self.inner.provider.as_ref()).await
    }

    /// A fresh data key, and its wrapped form to store, by `provider`.
    pub async fn generate_with(
        &self,
        provider: &dyn KeyProvider,
    ) -> Result<(DataKey, WrappedDataKey), Error> {
        let generation = self.generation();
        let data_key = DataKey(super::random_bytes::<32>()?);
        let wrapped = provider.wrap(&data_key.0).await?;
        self.cache_insert(cache_key(provider.name(), &wrapped), &data_key, &generation);
        Ok((data_key, wrapped))
    }

    /// The data key `stored` holds: from the cache, from the provider
    /// that wrapped it (`encryption_key_provider_changed` when that is not
    /// this one), or, for a legacy row, from the legacy master key
    /// (`encryption_legacy_master_key_required` without one).
    pub async fn unwrap(&self, stored: StoredWrap<'_>) -> Result<DataKey, Error> {
        match (stored.provider, stored.version) {
            (Some(provider), Some(version)) => {
                let Some(key) = self.provider_named(provider) else {
                    return Err(Error::ProviderChanged {
                        stored: provider.to_string(),
                        configured: self.provider_name().to_string(),
                    });
                };
                let wrapped = WrappedDataKey {
                    version,
                    ciphertext: stored.ciphertext.to_vec(),
                };
                let cache_key = cache_key(provider, &wrapped);
                // Join before admission, so identical bursts do not each
                // consume a provider slot. No blocking lock crosses an await.
                let (generation, existing) = {
                    let mut cache = self.inner.cache.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(data_key) = cache.get(&cache_key) {
                        return Ok(data_key);
                    }
                    (Arc::clone(&cache.generation), cache.flight(&cache_key))
                };
                if let Some(flight) = existing {
                    return flight.as_ref().clone().await;
                }
                let permit = Arc::clone(&self.inner.unwrap_slots)
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::ProviderUnavailable {
                        provider: provider.to_string(),
                        message: "unwrap admission closed".into(),
                    })?;
                let flight = {
                    let mut cache = self.inner.cache.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(data_key) = cache.get(&cache_key) {
                        return Ok(data_key);
                    }
                    if let Some(flight) = cache.flight(&cache_key) {
                        drop(permit);
                        flight
                    } else {
                        // Weak references retain neither results nor failures.
                        // Cancelled requests disappear on the next admission.
                        cache.flights.retain(|_, flight| flight.strong_count() > 0);
                        let service = self.clone();
                        let flight_generation = Arc::clone(&generation);
                        let flight: Flight = (Box::pin(async move {
                            let _permit = permit;
                            let result = key.unwrap(&wrapped).await.map(DataKey);
                            let mut cache = service
                                .inner
                                .cache
                                .lock()
                                .unwrap_or_else(|e| e.into_inner());
                            if Arc::ptr_eq(&cache.generation, &flight_generation) {
                                // Stop advertising completed work even if a
                                // waiter has not consumed its result yet.
                                cache.flights.remove(&cache_key);
                                if let Ok(data_key) = &result {
                                    cache.insert(cache_key, data_key.clone());
                                }
                            }
                            result
                        })
                            as super::provider::BoxFuture<'static, _>)
                            .shared();
                        let flight = Arc::new(flight);
                        // A request waiting for admission across invalidation
                        // may finish, but must not join the new generation.
                        if Arc::ptr_eq(&cache.generation, &generation) {
                            cache.flights.insert(cache_key, Arc::downgrade(&flight));
                        }
                        flight
                    }
                };
                flight.as_ref().clone().await
            }
            _ => {
                let legacy = self
                    .inner
                    .legacy
                    .as_ref()
                    .ok_or(Error::LegacyMasterKeyRequired)?;
                super::unwrap_data_key(legacy, stored.ciphertext, stored.nonce.unwrap_or(&[]))
            }
        }
    }

    /// The data key each of `stored` holds, one result per input, in
    /// order - [`unwrap`](Self::unwrap) for the rows of a rewrap batch
    /// (docs/architecture.md §246). The ones a key wrapped go to that key
    /// in one [`KeyProvider::unwrap_batch`], each key's batch under one of
    /// the service's unwrap slots; a legacy one is unwrapped here. A cached
    /// data key is answered from the cache, but nothing is added to it:
    /// what was read is about to be stored under another wrap, so an entry
    /// for it would only push out one in use.
    ///
    /// An error is per item: a batch its key couldn't answer has that
    /// error on each of its items.
    pub async fn unwrap_batch(&self, stored: &[StoredWrap<'_>]) -> Vec<Result<DataKey, Error>> {
        let mut out: Vec<Option<Result<DataKey, Error>>> = Vec::with_capacity(stored.len());
        // Per key: the key, the positions of its data keys and their wraps.
        type Batch = (Arc<dyn KeyProvider>, Vec<usize>, Vec<WrappedDataKey>);
        let mut batches: BTreeMap<&str, Batch> = BTreeMap::new();
        for (position, one) in stored.iter().enumerate() {
            let (Some(provider), Some(version)) = (one.provider, one.version) else {
                out.push(Some(match self.inner.legacy.as_ref() {
                    Some(legacy) => {
                        super::unwrap_data_key(legacy, one.ciphertext, one.nonce.unwrap_or(&[]))
                    }
                    None => Err(Error::LegacyMasterKeyRequired),
                }));
                continue;
            };
            let Some(key) = self.provider_named(provider) else {
                out.push(Some(Err(Error::ProviderChanged {
                    stored: provider.to_string(),
                    configured: self.provider_name().to_string(),
                })));
                continue;
            };
            let wrapped = WrappedDataKey {
                version,
                ciphertext: one.ciphertext.to_vec(),
            };
            let cached = self
                .inner
                .cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&cache_key(provider, &wrapped));
            if let Some(data_key) = cached {
                out.push(Some(Ok(data_key)));
                continue;
            }
            let batch = batches
                .entry(provider)
                .or_insert_with(|| (key, Vec::new(), Vec::new()));
            batch.1.push(position);
            batch.2.push(wrapped);
            out.push(None);
        }
        for (key, positions, wrapped) in batches.into_values() {
            let answered = match self.inner.unwrap_slots.acquire().await {
                Ok(_permit) => key.unwrap_batch(&wrapped).await,
                Err(_) => Err(Error::ProviderUnavailable {
                    provider: key.name().to_string(),
                    message: "unwrap admission closed".into(),
                }),
            };
            let answered = answered.and_then(|results| {
                if results.len() == positions.len() {
                    Ok(results)
                } else {
                    Err(Error::ProviderRefused {
                        provider: key.name().to_string(),
                        message: format!(
                            "unwrap_batch answered {} results for {} data keys",
                            results.len(),
                            positions.len()
                        ),
                    })
                }
            });
            match answered {
                Ok(results) => {
                    for (position, result) in positions.into_iter().zip(results) {
                        if let Some(slot) = out.get_mut(position) {
                            *slot = Some(result.map(DataKey));
                        }
                    }
                }
                Err(err) => {
                    for position in positions {
                        if let Some(slot) = out.get_mut(position) {
                            *slot = Some(Err(err.clone()));
                        }
                    }
                }
            }
        }
        out.into_iter()
            .map(|one| {
                one.unwrap_or_else(|| {
                    Err(Error::ProviderRefused {
                        provider: self.provider_name().to_string(),
                        message: "a data key of the batch was left unanswered".into(),
                    })
                })
            })
            .collect()
    }

    fn generation(&self) -> Arc<()> {
        Arc::clone(
            &self
                .inner
                .cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .generation,
        )
    }

    fn cache_insert(&self, key: Digest, data_key: &DataKey, generation: &Arc<()>) {
        let mut cache = self.inner.cache.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(&cache.generation, generation) {
            cache.insert(key, data_key.clone());
        }
    }

    /// Forgets every cached data key after retirement or deletion. Work
    /// already in flight can finish for its callers, but cannot refill
    /// this cache or be joined by requests starting after the clear.
    pub fn clear_cache(&self) {
        let mut cache = self.inner.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.generation = Arc::new(());
        cache.entries.clear();
        cache.clock.clear();
        cache.flights.clear();
    }
}

/// The cache is keyed by a digest of what is stored, so a row read again
/// after a rewrap or an erasure never finds an entry for what it used to
/// hold: the database is still read first, every time.
fn cache_key(provider: &str, wrapped: &WrappedDataKey) -> Digest {
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    context.update(&(provider.len() as u64).to_be_bytes());
    context.update(provider.as_bytes());
    context.update(&wrapped.version.to_be_bytes());
    context.update(&wrapped.ciphertext);
    let mut digest = [0; 32];
    digest.copy_from_slice(context.finish().as_ref());
    digest
}

#[cfg(test)]
mod tests;
