//! Key providers (docs/architecture.md §230): where the master key lives,
//! and the only code that ever uses it.
//!
//! skilj never holds the master key. It generates each subject's data key
//! itself (it needs it to seal the subject's fields) and hands it to a
//! [`KeyProvider`] to wrap; the wrapped form is what is stored. A provider
//! keeps *versions* of its key: it wraps under the latest, unwraps under
//! any version it still has, can rewrap a wrapped key under the latest
//! without handing the data key back, can add a version ([`rotate`]) and
//! can make every version below one unusable ([`retire_below`]) - the step
//! that makes a rotated-away key worthless, enforced by the provider
//! itself rather than by skilj choosing not to use it.
//!
//! [`VaultTransitProvider`](super::vault::VaultTransitProvider) keeps the
//! key in Vault or OpenBao. [`KeyFileProvider`] keeps it in a local file,
//! for local development and quick starts; [`InMemoryKeyProvider`] in
//! memory, for tests. Those two do hold the key in the process.
//!
//! [`rotate`]: KeyProvider::rotate
//! [`retire_below`]: KeyProvider::retire_below

use super::random_bytes;
use super::Error;
use futures_util::{StreamExt, TryStreamExt};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// The future a [`KeyProvider`] method answers with.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What [`KeyProvider::unwrap_batch`] answers: one result per data key
/// asked for, in order, or the error that failed the whole request.
pub type UnwrappedBatch = Result<Vec<Result<[u8; 32], Error>>, Error>;

/// What [`KeyProvider::batch_concurrency`] answers unless a provider says
/// otherwise: how many [`KeyProvider::unwrap`] or [`KeyProvider::wrap`]
/// calls the default [`KeyProvider::unwrap_batch`] and
/// [`KeyProvider::wrap_batch`] have in flight at once.
pub const SCALAR_BATCH_CONCURRENCY: usize = 8;

/// A data key as a [`KeyProvider`] wrapped it: the key version it is
/// wrapped under, and the provider's own opaque ciphertext.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrappedDataKey {
    /// The version of the master key it is wrapped under.
    pub version: u32,
    /// The provider's ciphertext, stored as it is.
    pub ciphertext: Vec<u8>,
}

/// A provider's key versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyVersions {
    /// The version new data keys are wrapped under.
    pub latest: u32,
    /// The oldest version that still unwraps; every one below is retired.
    pub min_decryption: u32,
}

/// Where the master key lives (see the module doc). Every method may talk
/// to a remote service, so each answers a future, and an implementation
/// is shared by every task of the process.
pub trait KeyProvider: Send + Sync + 'static {
    /// What is stored next to every data key this provider wrapped, and
    /// compared at startup: a provider that is not the one the data keys
    /// were wrapped by is refused. It names the key itself, not just the
    /// kind of provider, so pointing at another key is caught too.
    fn name(&self) -> &str;

    /// `data_key` wrapped under the latest version.
    fn wrap<'a>(&'a self, data_key: &'a [u8; 32]) -> BoxFuture<'a, Result<WrappedDataKey, Error>>;

    /// The data key `wrapped` holds. A retired version is refused.
    fn unwrap<'a>(&'a self, wrapped: &'a WrappedDataKey) -> BoxFuture<'a, Result<[u8; 32], Error>>;

    /// Each of `wrapped` wrapped anew under the latest version, one result
    /// per input, in order: one the key refuses doesn't fail the rest. A
    /// remote provider can do it without the data key leaving it.
    ///
    /// The default is [`unwrap_batch`](Self::unwrap_batch), then
    /// [`wrap_batch`](Self::wrap_batch) of the data keys that unwrapped:
    /// two batch requests for a provider with batch operations, and
    /// [`batch_concurrency`](Self::batch_concurrency) single calls at a
    /// time for one without. A failed wrap fails the whole batch, as
    /// `wrap_batch` does (docs/architecture.md §250).
    fn rewrap<'a>(
        &'a self,
        wrapped: &'a [WrappedDataKey],
    ) -> BoxFuture<'a, Result<Vec<Result<WrappedDataKey, Error>>, Error>> {
        Box::pin(async move {
            let unwrapped = sized(
                self.name(),
                wrapped.len(),
                self.unwrap_batch(wrapped).await?,
            )?;
            let mut data_keys = Vec::with_capacity(unwrapped.len());
            let mut out: Vec<Option<Result<WrappedDataKey, Error>>> =
                Vec::with_capacity(unwrapped.len());
            for result in unwrapped {
                match result {
                    Ok(data_key) => {
                        data_keys.push(data_key);
                        out.push(None);
                    }
                    Err(err) => out.push(Some(Err(err))),
                }
            }
            let mut rewrapped = sized(
                self.name(),
                data_keys.len(),
                self.wrap_batch(&data_keys).await?,
            )?
            .into_iter();
            Ok(out
                .into_iter()
                .map(|one| match one {
                    Some(refused) => refused,
                    None => rewrapped.next().ok_or_else(|| Error::ProviderRefused {
                        provider: self.name().to_string(),
                        message: "wrap_batch answered too few wraps".into(),
                    }),
                })
                .collect())
        })
    }

    /// How many single [`unwrap`](Self::unwrap) or [`wrap`](Self::wrap)
    /// calls the default [`unwrap_batch`](Self::unwrap_batch) and
    /// [`wrap_batch`](Self::wrap_batch) have in flight at once
    /// ([`SCALAR_BATCH_CONCURRENCY`] unless overridden; `0` counts as `1`).
    /// A move of data keys to another key is background work next to the
    /// reads and writes that use the same key: a provider behind a rate
    /// limit answers less here, or reads it from its own configuration.
    /// One that overrides both batch operations never has it asked.
    fn batch_concurrency(&self) -> usize {
        SCALAR_BATCH_CONCURRENCY
    }

    /// The data key each of `wrapped` holds, one result per input, in
    /// order: one that fails doesn't fail the rest. Moving a bounded
    /// context's data keys to another key reads them through this, a batch
    /// at a time (docs/architecture.md §246).
    ///
    /// The default calls [`unwrap`](Self::unwrap) for each, at most
    /// [`batch_concurrency`](Self::batch_concurrency) at a time, and
    /// answers each call's own result. A remote provider with a batch operation overrides it to
    /// make one request; it then answers `Err` for the whole batch only
    /// when the request itself failed (unreachable, not permitted), and a
    /// ciphertext it refuses as that item's [`Error::DecryptFailed`] or
    /// [`Error::KeyVersionRetired`] - the two that mark a data key
    /// unreadable. Any other error, per item or for the batch, ends the
    /// move for a later run to retry.
    fn unwrap_batch<'a>(&'a self, wrapped: &'a [WrappedDataKey]) -> BoxFuture<'a, UnwrappedBatch> {
        Box::pin(async move {
            // Made before they are streamed: nothing runs until polled.
            let calls: Vec<BoxFuture<'a, Result<[u8; 32], Error>>> =
                wrapped.iter().map(|one| self.unwrap(one)).collect();
            Ok(futures_util::stream::iter(calls)
                .buffered(self.batch_concurrency().max(1))
                .collect()
                .await)
        })
    }

    /// Each of `data_keys` wrapped under the latest version, in order. A
    /// wrap doesn't fail for one data key and not another, so one failure
    /// fails the batch, and nothing of it is stored.
    ///
    /// The default calls [`wrap`](Self::wrap) for each, at most
    /// [`batch_concurrency`](Self::batch_concurrency) at a time; a remote
    /// provider with a batch operation overrides it to make one request.
    fn wrap_batch<'a>(
        &'a self,
        data_keys: &'a [[u8; 32]],
    ) -> BoxFuture<'a, Result<Vec<WrappedDataKey>, Error>> {
        Box::pin(async move {
            let calls: Vec<BoxFuture<'a, Result<WrappedDataKey, Error>>> =
                data_keys.iter().map(|one| self.wrap(one)).collect();
            futures_util::stream::iter(calls)
                .buffered(self.batch_concurrency().max(1))
                .try_collect()
                .await
        })
    }

    /// The latest and oldest usable versions.
    fn versions(&self) -> BoxFuture<'_, Result<KeyVersions, Error>>;

    /// Adds a version and makes it the latest; answers it.
    fn rotate(&self) -> BoxFuture<'_, Result<u32, Error>>;

    /// Makes every version below `version` unusable for unwrapping.
    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), Error>>;

    /// Another key of the same provider, named `key` - a bounded context's
    /// own key (docs/architecture.md §230). Its [`name`](Self::name) names
    /// it apart from this one. The key need not exist yet:
    /// [`create`](Self::create) makes it.
    fn key(&self, key: &str) -> Result<std::sync::Arc<dyn KeyProvider>, Error>;

    /// Creates this key at the provider when it doesn't exist yet, and
    /// answers whether it did: `false` for a key that already existed,
    /// which skilj must then never delete (docs/architecture.md §230).
    fn create(&self) -> BoxFuture<'_, Result<bool, Error>>;

    /// Deletes this key at the provider, every version of it: whatever it
    /// wrapped can never be unwrapped again.
    fn delete(&self) -> BoxFuture<'_, Result<(), Error>>;
}

/// `batch` when it holds `expected` answers, and otherwise the provider's
/// refusal: a batch operation answering the wrong number is not trusted
/// with any of them.
pub(crate) fn sized<T>(provider: &str, expected: usize, batch: Vec<T>) -> Result<Vec<T>, Error> {
    if batch.len() == expected {
        Ok(batch)
    } else {
        Err(Error::ProviderRefused {
            provider: provider.to_string(),
            message: format!("answered {} results for {expected} inputs", batch.len()),
        })
    }
}

/// The one answer of a batch of one: how a provider whose batch operation
/// is the real one answers a single [`KeyProvider::wrap`] or
/// [`KeyProvider::unwrap`] (docs/architecture.md §250).
pub(crate) fn single<T>(provider: &str, batch: Vec<T>) -> Result<T, Error> {
    let mut batch = sized(provider, 1, batch)?.into_iter();
    batch.next().ok_or_else(|| Error::ProviderRefused {
        provider: provider.to_string(),
        message: "answered no result for one input".into(),
    })
}

/// Refuses a key name that isn't 1 to 64 ASCII letters, digits, `-` and
/// `_`: it ends up in a Vault path and a file name.
pub fn validate_key_name(key: &str) -> Result<(), Error> {
    let valid = !key.is_empty()
        && key.len() <= 64
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if valid {
        Ok(())
    } else {
        Err(Error::ProviderRefused {
            provider: "skilj".into(),
            message: format!("key name {key:?} must be 1 to 64 ASCII letters, digits, '-' or '_'"),
        })
    }
}

/// A keyring held in the process, shared by [`InMemoryKeyProvider`] and
/// [`KeyFileProvider`]: version `n` is `keys[n - 1]`, `None` once retired.
/// A wrapped key is the AES-256-GCM nonce followed by the sealed key.
#[derive(Clone, Serialize, Deserialize)]
struct Keyring {
    versions: Vec<StoredVersion>,
    min_decryption_version: u32,
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredVersion {
    version: u32,
    /// Hex; absent once retired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<String>,
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

impl Keyring {
    fn new(first: [u8; 32]) -> Self {
        Self {
            versions: vec![StoredVersion {
                version: 1,
                key: Some(to_hex(&first)),
            }],
            min_decryption_version: 1,
        }
    }

    fn latest(&self) -> u32 {
        self.versions.last().map_or(0, |v| v.version)
    }

    fn key(&self, version: u32) -> Result<[u8; 32], Error> {
        if version < self.min_decryption_version {
            return Err(Error::KeyVersionRetired { version });
        }
        let stored = self
            .versions
            .iter()
            .find(|v| v.version == version)
            .ok_or(Error::KeyVersionRetired { version })?;
        let hex = stored
            .key
            .as_deref()
            .ok_or(Error::KeyVersionRetired { version })?;
        from_hex(hex).ok_or_else(|| Error::ProviderRefused {
            provider: "local".into(),
            message: format!("version {version} is not a 32-byte hex key"),
        })
    }

    fn wrap(&self, data_key: &[u8; 32]) -> Result<WrappedDataKey, Error> {
        let version = self.latest();
        let key = self.key(version)?;
        let nonce = random_bytes::<NONCE_LEN>()?;
        let sealing = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &key).expect("a 32-byte key is valid for AES-256-GCM"),
        );
        let mut sealed = data_key.to_vec();
        sealing
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut sealed,
            )
            .expect("sealing a plain byte buffer under a fresh nonce never fails");
        let mut ciphertext = nonce.to_vec();
        ciphertext.extend_from_slice(&sealed);
        Ok(WrappedDataKey {
            version,
            ciphertext,
        })
    }

    fn unwrap(&self, wrapped: &WrappedDataKey) -> Result<[u8; 32], Error> {
        let key = self.key(wrapped.version)?;
        if wrapped.ciphertext.len() < NONCE_LEN {
            return Err(Error::DecryptFailed);
        }
        let (nonce, sealed) = wrapped.ciphertext.split_at(NONCE_LEN);
        let nonce: [u8; NONCE_LEN] = nonce.try_into().map_err(|_| Error::DecryptFailed)?;
        let opening = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &key).expect("a 32-byte key is valid for AES-256-GCM"),
        );
        let mut buffer = sealed.to_vec();
        let opened = opening
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut buffer,
            )
            .map_err(|_| Error::DecryptFailed)?;
        opened.try_into().map_err(|_| Error::DecryptFailed)
    }

    fn rotate(&mut self) -> Result<u32, Error> {
        let version = self.latest() + 1;
        self.versions.push(StoredVersion {
            version,
            key: Some(to_hex(&random_bytes::<32>()?)),
        });
        Ok(version)
    }

    fn retire_below(&mut self, version: u32) {
        let version = version.min(self.latest());
        for stored in &mut self.versions {
            if stored.version < version {
                stored.key = None;
            }
        }
        self.min_decryption_version = self.min_decryption_version.max(version);
    }

    fn versions(&self) -> KeyVersions {
        KeyVersions {
            latest: self.latest(),
            min_decryption: self.min_decryption_version,
        }
    }
}

/// A [`KeyProvider`] holding its keys in memory, for tests. Clones share
/// one set of keyrings, so two instances in a test can rotate together,
/// and [`key`](KeyProvider::key) addresses another keyring in the same set.
/// Nothing survives the process.
#[derive(Clone)]
pub struct InMemoryKeyProvider {
    keyrings: Arc<Mutex<std::collections::HashMap<String, Keyring>>>,
    key: String,
    name: String,
}

impl InMemoryKeyProvider {
    /// A keyring whose version 1 is a fresh random key.
    ///
    /// # Panics
    /// Panics if secure randomness is unavailable. Use [`Self::try_new`]
    /// to return the failure instead.
    pub fn new() -> Self {
        Self::try_new().expect("secure randomness is unavailable")
    }

    /// A fresh in-memory keyring, returning OS randomness failures.
    pub fn try_new() -> Result<Self, Error> {
        Ok(Self::from_bytes(random_bytes::<32>()?))
    }

    /// A keyring whose version 1 is `bytes`: two providers made from the
    /// same bytes unwrap each other's version-1 keys.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        let mut keyrings = std::collections::HashMap::new();
        keyrings.insert(String::new(), Keyring::new(bytes));
        Self {
            keyrings: Arc::new(Mutex::new(keyrings)),
            key: String::new(),
            name: "in-memory".into(),
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut Keyring) -> T) -> Result<T, Error> {
        let mut keyrings = self.keyrings.lock().unwrap_or_else(|e| e.into_inner());
        let keyring = keyrings
            .get_mut(&self.key)
            .ok_or_else(|| Error::ProviderRefused {
                provider: self.name.clone(),
                message: "the key does not exist".into(),
            })?;
        Ok(f(keyring))
    }
}

impl Default for InMemoryKeyProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyProvider for InMemoryKeyProvider {
    fn name(&self) -> &str {
        &self.name
    }

    /// A batch of one.
    fn wrap<'a>(&'a self, data_key: &'a [u8; 32]) -> BoxFuture<'a, Result<WrappedDataKey, Error>> {
        Box::pin(async move {
            single(
                &self.name,
                self.wrap_batch(std::slice::from_ref(data_key)).await?,
            )
        })
    }

    /// A batch of one.
    fn unwrap<'a>(&'a self, wrapped: &'a WrappedDataKey) -> BoxFuture<'a, Result<[u8; 32], Error>> {
        Box::pin(async move {
            single(
                &self.name,
                self.unwrap_batch(std::slice::from_ref(wrapped)).await?,
            )?
        })
    }

    /// The keyring locked once for the batch.
    fn unwrap_batch<'a>(&'a self, wrapped: &'a [WrappedDataKey]) -> BoxFuture<'a, UnwrappedBatch> {
        Box::pin(async move { self.with(|k| wrapped.iter().map(|one| k.unwrap(one)).collect()) })
    }

    /// The keyring locked once for the batch.
    fn wrap_batch<'a>(
        &'a self,
        data_keys: &'a [[u8; 32]],
    ) -> BoxFuture<'a, Result<Vec<WrappedDataKey>, Error>> {
        Box::pin(async move { self.with(|k| data_keys.iter().map(|one| k.wrap(one)).collect())? })
    }

    fn versions(&self) -> BoxFuture<'_, Result<KeyVersions, Error>> {
        Box::pin(async move { self.with(|k| k.versions()) })
    }

    fn rotate(&self) -> BoxFuture<'_, Result<u32, Error>> {
        Box::pin(async move { self.with(Keyring::rotate)? })
    }

    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move { self.with(|k| k.retire_below(version)) })
    }

    fn key(&self, key: &str) -> Result<Arc<dyn KeyProvider>, Error> {
        validate_key_name(key)?;
        Ok(Arc::new(Self {
            keyrings: Arc::clone(&self.keyrings),
            key: key.to_string(),
            name: format!("in-memory:{key}"),
        }))
    }

    fn create(&self) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            let mut keyrings = self.keyrings.lock().unwrap_or_else(|e| e.into_inner());
            if keyrings.contains_key(&self.key) {
                return Ok(false);
            }
            keyrings.insert(self.key.clone(), Keyring::new(random_bytes::<32>()?));
            Ok(true)
        })
    }

    fn delete(&self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.keyrings
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.key);
            Ok(())
        })
    }
}

/// A [`KeyProvider`] keeping its keys in a JSON file, for local
/// development and quick starts: created with a fresh key the first time,
/// a version appended by each rotation, and a retired version's key
/// removed from the file. Every instance sharing the file shares the key;
/// each operation re-reads it, so a rotation by one is seen by the others.
/// The key sits on disk and in the process: for production, use
/// [`VaultTransitProvider`](super::vault::VaultTransitProvider).
pub struct KeyFileProvider {
    path: PathBuf,
    /// Serialises this process's read-modify-writes of the file.
    lock: Mutex<()>,
    name: String,
}

impl KeyFileProvider {
    /// The provider for the file at `path`, creating it with a fresh key
    /// (readable by its owner only, on Unix) when it doesn't exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let provider = Self {
            path: path.as_ref().to_path_buf(),
            lock: Mutex::new(()),
            name: "file".into(),
        };
        provider.create_file()?;
        provider.read()?;
        Ok(provider)
    }

    /// Creates the file with a fresh key unless it exists; answers whether
    /// it did.
    fn create_file(&self) -> Result<bool, Error> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        if self.path.exists() {
            return Ok(false);
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| self.io_error(e))?;
        }
        self.write(&Keyring::new(random_bytes::<32>()?))?;
        Ok(true)
    }

    /// The file it reads.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn io_error(&self, err: impl std::fmt::Display) -> Error {
        Error::ProviderUnavailable {
            provider: self.name.clone(),
            message: format!("{}: {err}", self.path.display()),
        }
    }

    fn read(&self) -> Result<Keyring, Error> {
        let text = std::fs::read_to_string(&self.path).map_err(|e| self.io_error(e))?;
        serde_json::from_str(&text).map_err(|e| Error::ProviderRefused {
            provider: self.name.clone(),
            message: format!("{}: not a skilj key file: {e}", self.path.display()),
        })
    }

    fn write(&self, keyring: &Keyring) -> Result<(), Error> {
        let text = serde_json::to_string_pretty(keyring).expect("a keyring always serialises");
        let temporary = self.path.with_extension("tmp");
        {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create(true).truncate(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary).map_err(|e| self.io_error(e))?;
            std::io::Write::write_all(&mut file, text.as_bytes()).map_err(|e| self.io_error(e))?;
            file.sync_all().map_err(|e| self.io_error(e))?;
        }
        std::fs::rename(&temporary, &self.path).map_err(|e| self.io_error(e))
    }

    fn change<T>(&self, f: impl FnOnce(&mut Keyring) -> Result<T, Error>) -> Result<T, Error> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut keyring = self.read()?;
        let out = f(&mut keyring)?;
        self.write(&keyring)?;
        Ok(out)
    }
}

impl KeyProvider for KeyFileProvider {
    fn name(&self) -> &str {
        &self.name
    }

    /// A batch of one.
    fn wrap<'a>(&'a self, data_key: &'a [u8; 32]) -> BoxFuture<'a, Result<WrappedDataKey, Error>> {
        Box::pin(async move {
            single(
                &self.name,
                self.wrap_batch(std::slice::from_ref(data_key)).await?,
            )
        })
    }

    /// A batch of one.
    fn unwrap<'a>(&'a self, wrapped: &'a WrappedDataKey) -> BoxFuture<'a, Result<[u8; 32], Error>> {
        Box::pin(async move {
            single(
                &self.name,
                self.unwrap_batch(std::slice::from_ref(wrapped)).await?,
            )?
        })
    }

    /// The key file read once for the batch.
    fn unwrap_batch<'a>(&'a self, wrapped: &'a [WrappedDataKey]) -> BoxFuture<'a, UnwrappedBatch> {
        Box::pin(async move {
            let keyring = self.read()?;
            Ok(wrapped.iter().map(|one| keyring.unwrap(one)).collect())
        })
    }

    /// The key file read once for the batch.
    fn wrap_batch<'a>(
        &'a self,
        data_keys: &'a [[u8; 32]],
    ) -> BoxFuture<'a, Result<Vec<WrappedDataKey>, Error>> {
        Box::pin(async move {
            let keyring = self.read()?;
            data_keys.iter().map(|one| keyring.wrap(one)).collect()
        })
    }

    fn versions(&self) -> BoxFuture<'_, Result<KeyVersions, Error>> {
        Box::pin(async move { Ok(self.read()?.versions()) })
    }

    fn rotate(&self) -> BoxFuture<'_, Result<u32, Error>> {
        Box::pin(async move { self.change(Keyring::rotate) })
    }

    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.change(|k| {
                k.retire_below(version);
                Ok(())
            })
        })
    }

    /// The file next to this one, `<stem>-<key>.json`.
    fn key(&self, key: &str) -> Result<Arc<dyn KeyProvider>, Error> {
        validate_key_name(key)?;
        let stem = self
            .path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        Ok(Arc::new(Self {
            path: self.path.with_file_name(format!("{stem}-{key}.json")),
            lock: Mutex::new(()),
            name: format!("file:{key}"),
        }))
    }

    fn create(&self) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move { self.create_file() })
    }

    fn delete(&self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            match std::fs::remove_file(&self.path) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(self.io_error(e)),
                _ => Ok(()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn rng_failure_does_not_create_rotate_or_wrap_keys() {
        let provider = InMemoryKeyProvider::from_bytes([1; 32]);
        let path = std::env::temp_dir().join(format!(
            "skilj-rng-{}.json",
            crate::shared::generate_token_id()
        ));
        let file = KeyFileProvider::open(&path).unwrap();
        let original = std::fs::read(&path).unwrap();
        let missing = path.with_extension("missing");
        let service = super::super::KeyService::new(provider.clone());
        super::super::with_rng_failure(|| {
            block_on(async {
                assert!(matches!(
                    provider.wrap(&[2; 32]).await,
                    Err(Error::RandomnessUnavailable)
                ));
                assert!(matches!(
                    provider.rotate().await,
                    Err(Error::RandomnessUnavailable)
                ));
                assert!(matches!(
                    provider.key("new").unwrap().create().await,
                    Err(Error::RandomnessUnavailable)
                ));
                assert!(matches!(
                    service.generate().await,
                    Err(Error::RandomnessUnavailable)
                ));
                assert!(matches!(
                    file.wrap(&[2; 32]).await,
                    Err(Error::RandomnessUnavailable)
                ));
                assert!(matches!(
                    file.rotate().await,
                    Err(Error::RandomnessUnavailable)
                ));
                assert!(matches!(
                    KeyFileProvider::open(&missing),
                    Err(Error::RandomnessUnavailable)
                ));
            })
        });
        assert_eq!(block_on(provider.versions()).unwrap().latest, 1);
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(!missing.exists());
        assert!(block_on(provider.key("new").unwrap().versions()).is_err());
        assert_eq!(block_on(provider.rotate()).unwrap(), 2);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_rotated_keyring_still_unwraps_until_the_old_version_is_retired() {
        block_on(async {
            let provider = InMemoryKeyProvider::new();
            let data_key = [9u8; 32];
            let old = provider.wrap(&data_key).await.unwrap();
            assert_eq!(old.version, 1);
            assert_eq!(provider.rotate().await.unwrap(), 2);
            assert_eq!(provider.unwrap(&old).await.unwrap(), data_key);

            let mut rewrapped = provider.rewrap(std::slice::from_ref(&old)).await.unwrap();
            let new = rewrapped.remove(0).unwrap();
            assert_eq!(new.version, 2);

            provider.retire_below(2).await.unwrap();
            assert!(matches!(
                provider.unwrap(&old).await,
                Err(Error::KeyVersionRetired { version: 1 })
            ));
            assert_eq!(provider.unwrap(&new).await.unwrap(), data_key);
            assert_eq!(
                provider.versions().await.unwrap(),
                KeyVersions {
                    latest: 2,
                    min_decryption: 2
                }
            );
        });
    }

    /// A provider of one's own with only the single operations, failing
    /// the wraps it is told to: what the trait's defaults are for.
    struct Scalar {
        inner: InMemoryKeyProvider,
        unwraps: std::sync::atomic::AtomicUsize,
        wraps: std::sync::atomic::AtomicUsize,
        fail_wraps: bool,
    }

    impl Scalar {
        fn new(inner: InMemoryKeyProvider, fail_wraps: bool) -> Self {
            Self {
                inner,
                unwraps: Default::default(),
                wraps: Default::default(),
                fail_wraps,
            }
        }
    }

    impl KeyProvider for Scalar {
        fn name(&self) -> &str {
            "scalar"
        }
        fn wrap<'a>(
            &'a self,
            data_key: &'a [u8; 32],
        ) -> BoxFuture<'a, Result<WrappedDataKey, Error>> {
            self.wraps.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail_wraps {
                return Box::pin(async {
                    Err(Error::ProviderUnavailable {
                        provider: "scalar".into(),
                        message: "down".into(),
                    })
                });
            }
            self.inner.wrap(data_key)
        }
        fn unwrap<'a>(
            &'a self,
            wrapped: &'a WrappedDataKey,
        ) -> BoxFuture<'a, Result<[u8; 32], Error>> {
            self.unwraps
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.unwrap(wrapped)
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

    #[test]
    fn the_default_rewrap_unwraps_and_wraps_in_batches_and_keeps_a_refusal_per_item() {
        block_on(async {
            let inner = InMemoryKeyProvider::new();
            let provider = Scalar::new(inner.clone(), false);
            let a = inner.wrap(&[1; 32]).await.unwrap();
            let b = inner.wrap(&[2; 32]).await.unwrap();
            let bad = WrappedDataKey {
                version: 1,
                ciphertext: vec![0; 40],
            };
            inner.rotate().await.unwrap();

            let results = provider.rewrap(&[a, bad, b]).await.unwrap();
            assert_eq!(results.len(), 3);
            let first = results[0].as_ref().unwrap();
            assert_eq!(first.version, 2);
            assert_eq!(inner.unwrap(first).await.unwrap(), [1; 32]);
            assert!(
                matches!(results[1], Err(Error::DecryptFailed)),
                "{:?}",
                results[1]
            );
            let third = results[2].as_ref().unwrap();
            assert_eq!(inner.unwrap(third).await.unwrap(), [2; 32]);
            // Three unwraps and only the two readable ones wrapped.
            assert_eq!(
                provider.unwraps.load(std::sync::atomic::Ordering::SeqCst),
                3
            );
            assert_eq!(provider.wraps.load(std::sync::atomic::Ordering::SeqCst), 2);
            assert!(provider.rewrap(&[]).await.unwrap().is_empty());
        });
    }

    #[test]
    fn a_failed_wrap_fails_the_default_rewrap_as_a_batch() {
        block_on(async {
            let inner = InMemoryKeyProvider::new();
            let provider = Scalar::new(inner.clone(), true);
            let a = inner.wrap(&[1; 32]).await.unwrap();
            assert!(matches!(
                provider.rewrap(&[a.clone(), a]).await,
                Err(Error::ProviderUnavailable { .. })
            ));
            // Nothing to wrap: the refusals come back without a wrap asked.
            let bad = WrappedDataKey {
                version: 1,
                ciphertext: vec![0; 40],
            };
            let results = provider.rewrap(std::slice::from_ref(&bad)).await.unwrap();
            assert!(matches!(results[..], [Err(Error::DecryptFailed)]));
        });
    }

    #[test]
    fn a_batch_of_the_wrong_size_is_refused_whole() {
        assert_eq!(single("p", vec![7]).unwrap(), 7);
        assert!(matches!(
            single::<u8>("p", vec![]),
            Err(Error::ProviderRefused { .. })
        ));
        assert!(matches!(
            single("p", vec![1, 2]),
            Err(Error::ProviderRefused { .. })
        ));
        assert!(matches!(
            sized("p", 3, vec![1, 2]),
            Err(Error::ProviderRefused { .. })
        ));
    }

    #[test]
    fn single_calls_of_the_shipped_providers_answer_as_their_batches() {
        block_on(async {
            let provider = InMemoryKeyProvider::new();
            let wrapped = provider.wrap(&[5; 32]).await.unwrap();
            let batch = provider
                .unwrap_batch(&[wrapped.clone(), wrapped.clone()])
                .await
                .unwrap();
            assert!(batch.iter().all(|r| r.as_ref().unwrap() == &[5; 32]));
            assert_eq!(provider.unwrap(&wrapped).await.unwrap(), [5; 32]);
            // A key that doesn't exist: the batch's error, for one too.
            let missing = provider.key("missing").unwrap();
            assert!(matches!(
                missing.unwrap(&wrapped).await,
                Err(Error::ProviderRefused { .. })
            ));
            assert!(matches!(
                missing.wrap(&[5; 32]).await,
                Err(Error::ProviderRefused { .. })
            ));
        });
    }

    #[test]
    fn a_key_file_keeps_its_versions_and_drops_a_retired_key() {
        block_on(async {
            let path = std::env::temp_dir().join(format!(
                "skilj-key-file-{}.json",
                crate::shared::generate_token_id()
            ));
            let provider = KeyFileProvider::open(&path).unwrap();
            let data_key = [3u8; 32];
            let old = provider.wrap(&data_key).await.unwrap();
            provider.rotate().await.unwrap();

            // Another process opening the same file sees the rotation.
            let other = KeyFileProvider::open(&path).unwrap();
            assert_eq!(other.versions().await.unwrap().latest, 2);
            assert_eq!(other.unwrap(&old).await.unwrap(), data_key);

            other.retire_below(2).await.unwrap();
            let text = std::fs::read_to_string(&path).unwrap();
            let keyring: Keyring = serde_json::from_str(&text).unwrap();
            assert!(keyring.versions[0].key.is_none(), "the retired key is gone");
            assert!(matches!(
                provider.unwrap(&old).await,
                Err(Error::KeyVersionRetired { version: 1 })
            ));
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = std::fs::metadata(&path).unwrap().permissions().mode();
                assert_eq!(mode & 0o777, 0o600);
            }
            std::fs::remove_file(&path).unwrap();
        });
    }
}
