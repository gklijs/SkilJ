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
//! `decrypt_leaf` backs the real per-field, per-grant decrypt test both
//! `event_store::render_event`/`render_command` (a declared `SensitiveField`
//! names the exact leaf and subject) and `projections::read_projection`
//! (no declaration at all - `decrypt_ciphertext_leaves` below tries every
//! string leaf in a projection's own stored state against every key
//! resolved for the query's own instance `key`, safe to attempt broadly
//! since `decrypt_leaf`'s own AEAD tag makes a false-positive match
//! astronomically unlikely) both call into.

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use std::sync::Arc;

pub mod trail;

const KEY_LEN: usize = 32; // AES-256

/// Library-level errors this module's own operations reject for.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(
        "sensitive_fields declares a field to encrypt, but no encryption_master_key was \
         configured (SkiljBuilder::encryption_master_key)"
    )]
    /// A type declares sensitive fields, but no encryption master key is
    /// configured.
    MasterKeyNotConfigured,

    /// Covers both an `EncryptionKey`'s own wrapped `DataKey` failing to
    /// unwrap (corrupted/tampered stored bytes, or the wrong master key)
    /// and a leaf's own ciphertext failing to decrypt - `ring`'s own AEAD
    /// failure (`ring::error::Unspecified`) carries no further detail by
    /// design, so neither case can say more than "this didn't verify."
    #[error("AEAD decryption failed - corrupted or tampered ciphertext, or the wrong key")]
    DecryptFailed,

    /// A data key is wrapped under a master key this process doesn't have
    /// (docs/architecture.md §230): one rotated away from that is still in
    /// use, or the one in use when this process is configured with an
    /// older key only. Another instance, configured with it, can do the
    /// work.
    #[error(
        "a data key is wrapped under encryption master key {fingerprint}, which this process \
         doesn't have: configure it as encryption_master_key, or keep it in \
         previous_encryption_master_keys until a rotation has rewrapped every key"
    )]
    MasterKeyUnknown {
        /// The fingerprint of the master key the data key is wrapped under.
        fingerprint: String,
    },

    /// Startup found that the master key in use (docs/architecture.md
    /// §230) is none of the keys this process is configured with - a
    /// mistyped or regenerated key, which would otherwise wrap new data
    /// keys under a key nothing else has.
    #[error(
        "the encryption master key in use (fingerprint {fingerprint}) is not among the \
         configured keys: configure it as encryption_master_key, or, to rotate, configure \
         the new key there and this one in previous_encryption_master_keys"
    )]
    MasterKeyChanged {
        /// The fingerprint of the master key in use.
        fingerprint: String,
    },
}

impl crate::error::SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::MasterKeyNotConfigured => "encryption_master_key_not_configured",
            Error::DecryptFailed => "encryption_decrypt_failed",
            Error::MasterKeyUnknown { .. } => "encryption_master_key_unknown",
            Error::MasterKeyChanged { .. } => "encryption_master_key_changed",
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
///
/// It can carry the keys it replaced ([`with_previous`](Self::with_previous),
/// docs/architecture.md §230): a data key is stored with the
/// [`fingerprint`](Self::fingerprint) of the master key it is wrapped
/// under, and unwrapped with whichever configured key has it. A data key
/// stored before fingerprints were recorded is tried against every
/// configured key, the current one first.
#[derive(Clone)]
pub struct EncryptionMasterKey {
    /// The current key first, then the ones it replaced, newest first.
    keys: Arc<[MasterKeyBytes]>,
}

struct MasterKeyBytes {
    bytes: [u8; KEY_LEN],
    fingerprint: String,
}

/// The label a master key's fingerprint is an HMAC of. Fixed, so equal
/// keys always fingerprint alike, and unlike anything a data key's wrap
/// would seal, so the fingerprint says nothing about any wrapped key.
const FINGERPRINT_LABEL: &[u8] = b"skilj encryption master key fingerprint v1";

/// The first 16 bytes of the label's HMAC-SHA256 under `bytes`, in hex:
/// enough to tell keys apart, and revealing nothing about the key.
fn fingerprint_of(bytes: &[u8; KEY_LEN]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, bytes);
    hmac::sign(&key, FINGERPRINT_LABEL).as_ref()[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

impl EncryptionMasterKey {
    /// The key from its 32 bytes, which should come from a secure random
    /// source and be kept secret.
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self {
            keys: Arc::new([MasterKeyBytes {
                fingerprint: fingerprint_of(&bytes),
                bytes,
            }]),
        }
    }

    /// This key, still able to unwrap the data keys wrapped under each of
    /// `previous` - the keys it replaced, newest first (docs/architecture.md
    /// §230). Each one's own previous keys come along too; a key already
    /// present is not added twice.
    pub fn with_previous(self, previous: impl IntoIterator<Item = EncryptionMasterKey>) -> Self {
        let mut keys: Vec<MasterKeyBytes> = self
            .keys
            .iter()
            .map(|k| MasterKeyBytes {
                bytes: k.bytes,
                fingerprint: k.fingerprint.clone(),
            })
            .collect();
        for key in previous {
            for k in key.keys.iter() {
                if !keys.iter().any(|known| known.fingerprint == k.fingerprint) {
                    keys.push(MasterKeyBytes {
                        bytes: k.bytes,
                        fingerprint: k.fingerprint.clone(),
                    });
                }
            }
        }
        Self { keys: keys.into() }
    }

    /// The fingerprint of the current key: what is stored next to every
    /// data key wrapped under it. Not secret; equal for equal keys.
    pub fn fingerprint(&self) -> &str {
        &self.keys[0].fingerprint
    }

    /// The fingerprints of every configured key, the current one first.
    pub fn fingerprints(&self) -> impl Iterator<Item = &str> {
        self.keys.iter().map(|k| k.fingerprint.as_str())
    }

    /// Whether previous keys are configured: a rotation is, or was, under
    /// way.
    pub fn has_previous(&self) -> bool {
        self.keys.len() > 1
    }

    /// Whether `fingerprint` is one of the configured keys'.
    pub fn knows(&self, fingerprint: &str) -> bool {
        self.key(fingerprint).is_some()
    }

    fn key(&self, fingerprint: &str) -> Option<&[u8; KEY_LEN]> {
        self.keys
            .iter()
            .find(|k| k.fingerprint == fingerprint)
            .map(|k| &k.bytes)
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
/// row, wrapped under `master_key`'s current key - `db::get_or_create_encryption_key`'s
/// own "provision" branch. Returns both the plaintext `DataKey` (for the
/// caller to encrypt with immediately) and its wrapped bytes + nonce (for
/// the caller to persist - never the plaintext key itself).
pub fn generate_and_wrap_data_key(master_key: &EncryptionMasterKey) -> (DataKey, Vec<u8>, Vec<u8>) {
    let data_key = DataKey(random_bytes::<KEY_LEN>());
    let (wrapped, nonce) = seal_data_key(&master_key.keys[0].bytes, &data_key);
    (data_key, wrapped, nonce)
}

/// [`generate_and_wrap_data_key`] under the configured key whose
/// fingerprint is `fingerprint` - the master key in use, which during a
/// rotation (docs/architecture.md §230) may still be a previous one.
pub fn generate_and_wrap_data_key_under(
    master_key: &EncryptionMasterKey,
    fingerprint: &str,
) -> Result<(DataKey, Vec<u8>, Vec<u8>), Error> {
    let data_key = DataKey(random_bytes::<KEY_LEN>());
    let (wrapped, nonce) = rewrap_data_key(master_key, fingerprint, &data_key)?;
    Ok((data_key, wrapped, nonce))
}

/// `data_key` wrapped anew under the configured key whose fingerprint is
/// `fingerprint`, with a fresh nonce: a rotation's rewrap
/// (docs/architecture.md §230). The data key itself, and so every
/// ciphertext sealed under it, stays as it was.
pub fn rewrap_data_key(
    master_key: &EncryptionMasterKey,
    fingerprint: &str,
    data_key: &DataKey,
) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let key = master_key
        .key(fingerprint)
        .ok_or_else(|| Error::MasterKeyUnknown {
            fingerprint: fingerprint.to_string(),
        })?;
    Ok(seal_data_key(key, data_key))
}

fn seal_data_key(key: &[u8; KEY_LEN], data_key: &DataKey) -> (Vec<u8>, Vec<u8>) {
    let nonce_bytes = random_bytes::<NONCE_LEN>();
    let wrapped = seal(key, nonce_bytes, data_key.0.to_vec());
    (wrapped, nonce_bytes.to_vec())
}

/// Recovers a previously-wrapped `DataKey` - `db::get_or_create_encryption_key`'s
/// own "reuse an already-active key" branch - trying every configured key,
/// the current one first.
pub fn unwrap_data_key(
    master_key: &EncryptionMasterKey,
    wrapped: &[u8],
    nonce: &[u8],
) -> Result<DataKey, Error> {
    unwrap_data_key_under(master_key, None, wrapped, nonce).map(|(data_key, _)| data_key)
}

/// [`unwrap_data_key`] for a data key stored with the fingerprint of the
/// master key it is wrapped under (docs/architecture.md §230): unwrapped
/// with that key, or refused with [`Error::MasterKeyUnknown`] when it is
/// not configured. `None`, a data key stored before fingerprints were,
/// tries every configured key, the current one first. Also answers the
/// fingerprint of the key that unwrapped it.
pub fn unwrap_data_key_under(
    master_key: &EncryptionMasterKey,
    fingerprint: Option<&str>,
    wrapped: &[u8],
    nonce: &[u8],
) -> Result<(DataKey, String), Error> {
    let nonce_bytes: [u8; NONCE_LEN] = nonce.try_into().map_err(|_| Error::DecryptFailed)?;
    let unwrap_with = |key: &[u8; KEY_LEN]| -> Result<DataKey, Error> {
        let opened = open(key, nonce_bytes, wrapped.to_vec())?;
        let key_bytes: [u8; KEY_LEN] = opened.try_into().map_err(|_| Error::DecryptFailed)?;
        Ok(DataKey(key_bytes))
    };
    match fingerprint {
        Some(fingerprint) => {
            let key = master_key
                .key(fingerprint)
                .ok_or_else(|| Error::MasterKeyUnknown {
                    fingerprint: fingerprint.to_string(),
                })?;
            Ok((unwrap_with(key)?, fingerprint.to_string()))
        }
        None => master_key
            .keys
            .iter()
            .find_map(|k| {
                unwrap_with(&k.bytes)
                    .ok()
                    .map(|data_key| (data_key, k.fingerprint.clone()))
            })
            .ok_or(Error::DecryptFailed),
    }
}

/// Encrypts one sensitive field's own plaintext leaf value under its
/// subject's `DataKey` - `event_store::protect_sensitive_fields`'s own
/// per-field operation. A fresh random nonce every call, prepended to the
/// ciphertext+tag and base64-encoded as a single opaque string - the
/// value that replaces the leaf's plaintext in the stored payload.
pub fn encrypt_leaf(data_key: &DataKey, plaintext: &str) -> String {
    let nonce_bytes = random_bytes::<NONCE_LEN>();
    let sealed = seal(&data_key.0, nonce_bytes, plaintext.as_bytes().to_vec());
    let mut combined = Vec::with_capacity(NONCE_LEN + sealed.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&sealed);
    crate::base64::encode(&combined)
}

/// Inverts `encrypt_leaf`.
pub fn decrypt_leaf(data_key: &DataKey, ciphertext_b64: &str) -> Result<String, Error> {
    let combined = crate::base64::decode(ciphertext_b64).ok_or(Error::DecryptFailed)?;
    if combined.len() < NONCE_LEN {
        return Err(Error::DecryptFailed);
    }
    let (nonce_bytes, ciphertext) = combined.split_at(NONCE_LEN);
    let nonce_bytes: [u8; NONCE_LEN] = nonce_bytes.try_into().map_err(|_| Error::DecryptFailed)?;
    let opened = open(&data_key.0, nonce_bytes, ciphertext.to_vec())?;
    String::from_utf8(opened).map_err(|_| Error::DecryptFailed)
}

/// `projections::read_projection`'s own real branch - real decrypt-on-read
/// for `Projection` state, with no field declared sensitive anywhere on
/// `Projection` itself (see `docs/architecture.md`'s own write-up of this
/// pass, and `specs/skilj.allium`'s `SensitiveFieldsStayProtected`
/// guarantee on `ProjectionQuery`: "sensitivity is declared once, on the
/// EventType, and inherited rather than re-declared per projection" -
/// this is what actually delivers that, rather than asking a projection
/// author to redeclare it, a design a declared-`sensitive_fields` draft
/// was rejected for: an author who forgets to declare a field that
/// really does hold sensitive content would otherwise leak it to every
/// caller regardless of grant, silently).
///
/// Recurses into every object/array, trying `decrypt_leaf` against each
/// of `data_keys` for every string leaf it finds, at any depth - no
/// "one level of nesting" cap the way a *declared* `SensitiveField.field`
/// path needs one for: there's no static path to validate at
/// registration time here, so there's nothing stopping a full, generic
/// walk, which is also strictly more thorough. A leaf that decrypts under
/// none of `data_keys` is left completely untouched - genuinely plain
/// data, or ciphertext for some other subject either way; a leaf that
/// decrypts under one is real ciphertext for the subject those keys were
/// resolved for, substituted with its plaintext. Every candidate key is
/// tried in turn and the first successful one wins - `data_keys` is
/// expected to be small (the number of distinct subjects the query's own
/// instance `key` matches across every `subject_key` namespace, normally
/// one), so this stays cheap even though it isn't declaratively scoped to
/// one exact field.
pub fn decrypt_ciphertext_leaves(value: &mut serde_json::Value, data_keys: &[DataKey]) {
    match value {
        serde_json::Value::String(s) => {
            for data_key in data_keys {
                if let Ok(plaintext) = decrypt_leaf(data_key, s) {
                    *s = plaintext;
                    return;
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                decrypt_ciphertext_leaves(item, data_keys);
            }
        }
        serde_json::Value::Object(fields) => {
            for field in fields.values_mut() {
                decrypt_ciphertext_leaves(field, data_keys);
            }
        }
        _ => {}
    }
}
