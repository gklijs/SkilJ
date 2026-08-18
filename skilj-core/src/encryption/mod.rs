//! Envelope encryption for `SensitiveField`/`EncryptionKey` - see
//! `docs/architecture.md`'s own write-up of this pass and
//! `specs/skilj.allium`'s "the encryption scheme itself (algorithm, key
//! derivation, physical key storage)... is a black box, consistent with
//! how `AccessToken.secret` generation and JWT signature verification are
//! already treated" note (§Excludes). Uses `ring` (already in the
//! dependency tree transitively via `reqwest`'s rustls-tls - promoted to
//! a direct dependency here, not a new crate) rather than hand-rolled
//! crypto.
//!
//! Two-layer (envelope) scheme: each `EncryptionKey` row gets its own
//! random 256-bit AES-256-GCM `DataKey`, generated once and never
//! persisted in the clear - it's wrapped (encrypted) under a single,
//! process-wide `EncryptionMasterKey` (supplied once via
//! `SkiljBuilder::encryption_master_key`, the same register `IdpConfig`
//! already lives in) and only the wrapped bytes are stored
//! (`db::get_or_create_encryption_key`). A sensitive field's own
//! plaintext leaf is sealed under the *data* key, not the master key
//! directly - destroying one subject's `EncryptionKey` (nulling its
//! wrapped data key - see `db::destroy_encryption_key`) only ever affects
//! that one subject's own ciphertext, never anyone else's, even though
//! every subject in a process shares the same master key. Consistent with
//! the spec's own stated threat model: protecting *application-level*
//! reads, not a raw-Postgres-access attacker - an operator with direct
//! Postgres access already has full access, per the spec's own text
//! elsewhere on why "what's still plaintext" isn't a query this library
//! offers.
//!
//! `decrypt_leaf` exists and is tested (round-tripping `encrypt_leaf`) but
//! has no production caller yet - the real per-field, per-grant decrypt
//! test (`render_event`/`render_command`'s own eventual non-trivial
//! branch) is deferred to whichever pass builds real decrypt-on-read; see
//! `event_store::render_event`'s own doc comment.

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};

const KEY_LEN: usize = 32; // AES-256

/// Library-level errors this module's own operations reject for.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(
        "sensitive_fields declares a field to encrypt, but no encryption_master_key was \
         configured (SkiljBuilder::encryption_master_key)"
    )]
    MasterKeyNotConfigured,

    /// Covers both an `EncryptionKey`'s own wrapped `DataKey` failing to
    /// unwrap (corrupted/tampered stored bytes, or the wrong master key)
    /// and a leaf's own ciphertext failing to decrypt - `ring`'s own AEAD
    /// failure (`ring::error::Unspecified`) carries no further detail by
    /// design, so neither case can say more than "this didn't verify."
    #[error("AEAD decryption failed - corrupted or tampered ciphertext, or the wrong key")]
    DecryptFailed,
}

impl crate::error::SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::MasterKeyNotConfigured => "encryption_master_key_not_configured",
            Error::DecryptFailed => "encryption_decrypt_failed",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

/// A process-wide symmetric key wrapping every per-subject `DataKey` this
/// process ever provisions - supplied once, at `SkiljBuilder::
/// encryption_master_key(...)` time. Never itself persisted; a consuming
/// application owns keeping this value stable across restarts - losing it
/// makes every already-wrapped `DataKey` permanently unrecoverable, the
/// same "physical key storage" concern the spec leaves entirely to this
/// module's own discretion.
#[derive(Clone)]
pub struct EncryptionMasterKey([u8; KEY_LEN]);

impl EncryptionMasterKey {
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }
}

impl std::fmt::Debug for EncryptionMasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncryptionMasterKey(REDACTED)")
    }
}

/// One subject's own per-`EncryptionKey` symmetric key - generated once,
/// at provisioning time (`db::get_or_create_encryption_key`), and never
/// itself persisted in the clear; only its *wrapped* form
/// (`generate_and_wrap_data_key`'s own output) is stored. Deliberately no
/// `Debug` impl - nothing should ever have a reason to print one.
#[derive(Clone)]
pub struct DataKey([u8; KEY_LEN]);

fn random_bytes<const N: usize>() -> [u8; N] {
    let rng = SystemRandom::new();
    let mut bytes = [0u8; N];
    rng.fill(&mut bytes).expect(
        "SystemRandom::fill only fails on catastrophic OS RNG failure - see ring's own docs",
    );
    bytes
}

fn seal(key_bytes: &[u8; KEY_LEN], nonce_bytes: [u8; NONCE_LEN], mut in_out: Vec<u8>) -> Vec<u8> {
    let unbound = UnboundKey::new(&AES_256_GCM, key_bytes)
        .expect("a KEY_LEN=32-byte key is always valid for AES_256_GCM");
    let key = LessSafeKey::new(unbound);
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .expect("sealing a plain byte buffer under a fresh nonce never fails");
    in_out
}

fn open(
    key_bytes: &[u8; KEY_LEN],
    nonce_bytes: [u8; NONCE_LEN],
    mut in_out: Vec<u8>,
) -> Result<Vec<u8>, Error> {
    let unbound = UnboundKey::new(&AES_256_GCM, key_bytes)
        .expect("a KEY_LEN=32-byte key is always valid for AES_256_GCM");
    let key = LessSafeKey::new(unbound);
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let len = key
        .open_in_place(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| Error::DecryptFailed)?
        .len();
    in_out.truncate(len);
    Ok(in_out)
}

/// Generates a fresh `DataKey` for a newly-provisioned `EncryptionKey`
/// row, wrapped under `master_key` - `db::get_or_create_encryption_key`'s
/// own "provision" branch. Returns both the plaintext `DataKey` (for the
/// caller to encrypt with immediately) and its wrapped bytes + nonce (for
/// the caller to persist - never the plaintext key itself).
pub fn generate_and_wrap_data_key(master_key: &EncryptionMasterKey) -> (DataKey, Vec<u8>, Vec<u8>) {
    let data_key_bytes = random_bytes::<KEY_LEN>();
    let nonce_bytes = random_bytes::<NONCE_LEN>();
    let wrapped = seal(&master_key.0, nonce_bytes, data_key_bytes.to_vec());
    (DataKey(data_key_bytes), wrapped, nonce_bytes.to_vec())
}

/// Recovers a previously-wrapped `DataKey` - `db::get_or_create_encryption_key`'s
/// own "reuse an already-active key" branch.
pub fn unwrap_data_key(
    master_key: &EncryptionMasterKey,
    wrapped: &[u8],
    nonce: &[u8],
) -> Result<DataKey, Error> {
    let nonce_bytes: [u8; NONCE_LEN] = nonce.try_into().map_err(|_| Error::DecryptFailed)?;
    let opened = open(&master_key.0, nonce_bytes, wrapped.to_vec())?;
    let key_bytes: [u8; KEY_LEN] = opened.try_into().map_err(|_| Error::DecryptFailed)?;
    Ok(DataKey(key_bytes))
}

/// Encrypts one sensitive field's own plaintext leaf value under its
/// subject's `DataKey` - `event_store::protect_sensitive_fields`'s own
/// per-field operation. A fresh random nonce every call, prepended to the
/// ciphertext+tag and base64-encoded as a single opaque string - the
/// value that replaces the leaf's plaintext in the stored payload.
pub fn encrypt_leaf(data_key: &DataKey, plaintext: &str) -> String {
    use base64::Engine;
    let nonce_bytes = random_bytes::<NONCE_LEN>();
    let sealed = seal(&data_key.0, nonce_bytes, plaintext.as_bytes().to_vec());
    let mut combined = Vec::with_capacity(NONCE_LEN + sealed.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&sealed);
    base64::engine::general_purpose::STANDARD.encode(combined)
}

/// Inverts `encrypt_leaf` - see this module's own doc comment for why
/// nothing calls this in production yet.
pub fn decrypt_leaf(data_key: &DataKey, ciphertext_b64: &str) -> Result<String, Error> {
    use base64::Engine;
    let combined = base64::engine::general_purpose::STANDARD
        .decode(ciphertext_b64)
        .map_err(|_| Error::DecryptFailed)?;
    if combined.len() < NONCE_LEN {
        return Err(Error::DecryptFailed);
    }
    let (nonce_bytes, ciphertext) = combined.split_at(NONCE_LEN);
    let nonce_bytes: [u8; NONCE_LEN] = nonce_bytes.try_into().map_err(|_| Error::DecryptFailed)?;
    let opened = open(&data_key.0, nonce_bytes, ciphertext.to_vec())?;
    String::from_utf8(opened).map_err(|_| Error::DecryptFailed)
}
