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

use super::Error;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// The future a [`KeyProvider`] method answers with.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
    /// per input, in order: one that fails doesn't fail the rest. The
    /// default unwraps and wraps; a remote provider can do it without the
    /// data key leaving it.
    fn rewrap<'a>(
        &'a self,
        wrapped: &'a [WrappedDataKey],
    ) -> BoxFuture<'a, Result<Vec<Result<WrappedDataKey, Error>>, Error>> {
        Box::pin(async move {
            let mut out = Vec::with_capacity(wrapped.len());
            for one in wrapped {
                let rewrapped = match self.unwrap(one).await {
                    Ok(data_key) => self.wrap(&data_key).await,
                    Err(err) => Err(err),
                };
                out.push(rewrapped);
            }
            Ok(out)
        })
    }

    /// The latest and oldest usable versions.
    fn versions(&self) -> BoxFuture<'_, Result<KeyVersions, Error>>;

    /// Adds a version and makes it the latest; answers it.
    fn rotate(&self) -> BoxFuture<'_, Result<u32, Error>>;

    /// Makes every version below `version` unusable for unwrapping.
    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), Error>>;
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    SystemRandom::new().fill(&mut bytes).expect(
        "SystemRandom::fill only fails on catastrophic OS RNG failure - see ring's own docs",
    );
    bytes
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
        let nonce = random_bytes::<NONCE_LEN>();
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

    fn rotate(&mut self) -> u32 {
        let version = self.latest() + 1;
        self.versions.push(StoredVersion {
            version,
            key: Some(to_hex(&random_bytes::<32>())),
        });
        version
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
/// one keyring, so two instances in a test can rotate together. Nothing
/// survives the process.
#[derive(Clone)]
pub struct InMemoryKeyProvider {
    keyring: Arc<Mutex<Keyring>>,
}

impl InMemoryKeyProvider {
    /// A keyring whose version 1 is a fresh random key.
    pub fn new() -> Self {
        Self::from_bytes(random_bytes::<32>())
    }

    /// A keyring whose version 1 is `bytes`: two providers made from the
    /// same bytes unwrap each other's version-1 keys.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self {
            keyring: Arc::new(Mutex::new(Keyring::new(bytes))),
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut Keyring) -> T) -> T {
        f(&mut self.keyring.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

impl Default for InMemoryKeyProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyProvider for InMemoryKeyProvider {
    fn name(&self) -> &str {
        "in-memory"
    }

    fn wrap<'a>(&'a self, data_key: &'a [u8; 32]) -> BoxFuture<'a, Result<WrappedDataKey, Error>> {
        Box::pin(async move { self.with(|k| k.wrap(data_key)) })
    }

    fn unwrap<'a>(&'a self, wrapped: &'a WrappedDataKey) -> BoxFuture<'a, Result<[u8; 32], Error>> {
        Box::pin(async move { self.with(|k| k.unwrap(wrapped)) })
    }

    fn versions(&self) -> BoxFuture<'_, Result<KeyVersions, Error>> {
        Box::pin(async move { Ok(self.with(|k| k.versions())) })
    }

    fn rotate(&self) -> BoxFuture<'_, Result<u32, Error>> {
        Box::pin(async move { Ok(self.with(Keyring::rotate)) })
    }

    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.with(|k| k.retire_below(version));
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
}

impl KeyFileProvider {
    /// The provider for the file at `path`, creating it with a fresh key
    /// (readable by its owner only, on Unix) when it doesn't exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let provider = Self {
            path: path.as_ref().to_path_buf(),
            lock: Mutex::new(()),
        };
        if !provider.path.exists() {
            if let Some(parent) = provider.path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| provider.io_error(e))?;
            }
            provider.write(&Keyring::new(random_bytes::<32>()))?;
        }
        provider.read()?;
        Ok(provider)
    }

    /// The file it reads.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn io_error(&self, err: impl std::fmt::Display) -> Error {
        Error::ProviderUnavailable {
            provider: "file".into(),
            message: format!("{}: {err}", self.path.display()),
        }
    }

    fn read(&self) -> Result<Keyring, Error> {
        let text = std::fs::read_to_string(&self.path).map_err(|e| self.io_error(e))?;
        serde_json::from_str(&text).map_err(|e| Error::ProviderRefused {
            provider: "file".into(),
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

    fn change<T>(&self, f: impl FnOnce(&mut Keyring) -> T) -> Result<T, Error> {
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut keyring = self.read()?;
        let out = f(&mut keyring);
        self.write(&keyring)?;
        Ok(out)
    }
}

impl KeyProvider for KeyFileProvider {
    fn name(&self) -> &str {
        "file"
    }

    fn wrap<'a>(&'a self, data_key: &'a [u8; 32]) -> BoxFuture<'a, Result<WrappedDataKey, Error>> {
        Box::pin(async move { self.read()?.wrap(data_key) })
    }

    fn unwrap<'a>(&'a self, wrapped: &'a WrappedDataKey) -> BoxFuture<'a, Result<[u8; 32], Error>> {
        Box::pin(async move { self.read()?.unwrap(wrapped) })
    }

    fn versions(&self) -> BoxFuture<'_, Result<KeyVersions, Error>> {
        Box::pin(async move { Ok(self.read()?.versions()) })
    }

    fn rotate(&self) -> BoxFuture<'_, Result<u32, Error>> {
        Box::pin(async move { self.change(Keyring::rotate) })
    }

    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move { self.change(|k| k.retire_below(version)) })
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
