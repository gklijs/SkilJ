//! [`KeyService`]: the handle every path that seals or opens a sensitive
//! field goes through (docs/architecture.md §230) - the configured
//! [`KeyProvider`], a short-lived cache of unwrapped data keys, and, while
//! a deployment still has them, the raw master key that wrapped data keys
//! before key providers existed.

use super::provider::{KeyProvider, WrappedDataKey};
use super::{DataKey, EncryptionMasterKey, Error};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

struct Cache {
    entries: HashMap<Vec<u8>, (DataKey, Instant)>,
    ttl: Duration,
    capacity: usize,
}

impl Cache {
    fn get(&mut self, key: &[u8]) -> Option<DataKey> {
        let (data_key, at) = self.entries.get(key)?;
        if at.elapsed() < self.ttl {
            Some(data_key.clone())
        } else {
            self.entries.remove(key);
            None
        }
    }

    fn insert(&mut self, key: Vec<u8>, data_key: DataKey) {
        if self.capacity == 0 {
            return;
        }
        if self.entries.len() >= self.capacity {
            let ttl = self.ttl;
            self.entries.retain(|_, (_, at)| at.elapsed() < ttl);
            if self.entries.len() >= self.capacity {
                // Still full of live keys: start over rather than track
                // recency for every read. The next reads pay one unwrap.
                self.entries.clear();
            }
        }
        self.entries.insert(key, (data_key, Instant::now()));
    }
}

struct Inner {
    provider: Arc<dyn KeyProvider>,
    legacy: Option<EncryptionMasterKey>,
    cache: Mutex<Cache>,
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
        Self {
            inner: Arc::new(Inner {
                provider,
                legacy,
                cache: Mutex::new(Cache {
                    entries: HashMap::new(),
                    ttl,
                    capacity,
                }),
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
    /// `capacity` of them; a capacity of 0 turns the cache off.
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

    /// The provider.
    pub fn provider(&self) -> &dyn KeyProvider {
        self.inner.provider.as_ref()
    }

    /// The provider's name, as stored next to each data key it wraps.
    pub fn provider_name(&self) -> &str {
        self.inner.provider.name()
    }

    /// Whether a legacy master key is configured.
    pub fn has_legacy_master_key(&self) -> bool {
        self.inner.legacy.is_some()
    }

    /// A fresh data key, and its wrapped form to store.
    pub async fn generate(&self) -> Result<(DataKey, WrappedDataKey), Error> {
        let data_key = DataKey(super::random_bytes::<32>());
        let wrapped = self.inner.provider.wrap(&data_key.0).await?;
        self.remember(self.provider_name(), &wrapped, &data_key);
        Ok((data_key, wrapped))
    }

    /// The data key `stored` holds: from the cache, from the provider
    /// that wrapped it (`encryption_key_provider_changed` when that is not
    /// this one), or, for a legacy row, from the legacy master key
    /// (`encryption_legacy_master_key_required` without one).
    pub async fn unwrap(&self, stored: StoredWrap<'_>) -> Result<DataKey, Error> {
        match (stored.provider, stored.version) {
            (Some(provider), Some(version)) => {
                if provider != self.provider_name() {
                    return Err(Error::ProviderChanged {
                        stored: provider.to_string(),
                        configured: self.provider_name().to_string(),
                    });
                }
                let wrapped = WrappedDataKey {
                    version,
                    ciphertext: stored.ciphertext.to_vec(),
                };
                let cache_key = cache_key(provider, &wrapped);
                if let Some(data_key) = self.cached(&cache_key) {
                    return Ok(data_key);
                }
                let data_key = DataKey(self.inner.provider.unwrap(&wrapped).await?);
                self.cache_insert(cache_key, &data_key);
                Ok(data_key)
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

    fn remember(&self, provider: &str, wrapped: &WrappedDataKey, data_key: &DataKey) {
        self.cache_insert(cache_key(provider, wrapped), data_key);
    }

    fn cached(&self, key: &[u8]) -> Option<DataKey> {
        self.inner
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
    }

    fn cache_insert(&self, key: Vec<u8>, data_key: &DataKey) {
        self.inner
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, data_key.clone());
    }

    /// Forgets every cached data key - after a version is retired, so no
    /// key unwrapped under it outlives the retirement in memory.
    pub fn clear_cache(&self) {
        self.inner
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .clear();
    }
}

/// The cache is keyed by a digest of what is stored, so a row read again
/// after a rewrap or an erasure never finds an entry for what it used to
/// hold: the database is still read first, every time.
fn cache_key(provider: &str, wrapped: &WrappedDataKey) -> Vec<u8> {
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    context.update(&(provider.len() as u64).to_be_bytes());
    context.update(provider.as_bytes());
    context.update(&wrapped.version.to_be_bytes());
    context.update(&wrapped.ciphertext);
    context.finish().as_ref().to_vec()
}
