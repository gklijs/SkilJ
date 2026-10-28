//! The encryption scheme, token secrets and blind tags - all black boxes in
//! the spec ("protect_sensitive_fields", "generate_token_secret",
//! TagMapping.blind).
//!
//! * Each EncryptionKey is a random 256-bit data key, stored wrapped by a
//!   master key provider (`MasterKeyProvider`); the master key never sits in
//!   the database.
//! * A sensitive leaf is replaced by `skilj:enc:v1:<key id>:<base64>` where
//!   the base64 holds nonce || AES-256-GCM(JSON of the leaf), with the key
//!   id as associated data.
//! * Token secrets are 256-bit random values, stored as SHA-256 (they are
//!   high entropy, so no salt/KDF is needed) and compared in constant time.
//! * Blind tags are HMAC-SHA256 under a key derived per (bounded context,
//!   tag key) from a configured root key, over the normalized value.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::tags::BlindHasher;

pub const CIPHERTEXT_PREFIX: &str = "skilj:enc:v1:";

/// Wraps and unwraps data keys. Implementations: a local key (below), or a
/// remote service such as Vault Transit (not built here).
pub trait MasterKeyProvider: Send + Sync {
    /// Identifier of the master key generation, stored beside each wrapped key.
    fn key_id(&self) -> String;
    fn wrap(&self, data_key: &[u8]) -> Result<Vec<u8>>;
    fn unwrap(&self, key_id: &str, wrapped: &[u8]) -> Result<Vec<u8>>;
}

/// A master key held in process memory, loaded from configuration.
pub struct LocalMasterKey {
    id: String,
    cipher: Aes256Gcm,
}

impl LocalMasterKey {
    pub fn new(id: impl Into<String>, key: [u8; 32]) -> Self {
        LocalMasterKey {
            id: id.into(),
            cipher: Aes256Gcm::new_from_slice(&key).expect("32-byte key"),
        }
    }

    /// From a base64-encoded 32-byte key.
    pub fn from_base64(id: impl Into<String>, b64: &str) -> Result<Self> {
        let bytes = STANDARD
            .decode(b64.trim())
            .map_err(|e| Error::Config(format!("master key is not base64: {e}")))?;
        let key: [u8; 32] = bytes
            .try_into()
            .map_err(|_| Error::Config("master key must be 32 bytes".into()))?;
        Ok(Self::new(id, key))
    }
}

impl MasterKeyProvider for LocalMasterKey {
    fn key_id(&self) -> String {
        self.id.clone()
    }
    fn wrap(&self, data_key: &[u8]) -> Result<Vec<u8>> {
        seal(&self.cipher, data_key, self.id.as_bytes())
    }
    fn unwrap(&self, key_id: &str, wrapped: &[u8]) -> Result<Vec<u8>> {
        if key_id != self.id {
            return Err(Error::Crypto(format!(
                "data key wrapped under unknown master key '{key_id}'"
            )));
        }
        open(&self.cipher, wrapped, self.id.as_bytes())
    }
}

fn seal(cipher: &Aes256Gcm, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce);
    let ct = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| Error::Crypto("encryption failed".into()))?;
    let mut out = nonce.to_vec();
    out.extend(ct);
    Ok(out)
}

fn open(cipher: &Aes256Gcm, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    if sealed.len() < 12 {
        return Err(Error::Crypto("ciphertext too short".into()));
    }
    let (nonce, ct) = sealed.split_at(12);
    cipher
        .decrypt(Nonce::from_slice(nonce), Payload { msg: ct, aad })
        .map_err(|_| Error::Crypto("decryption failed".into()))
}

pub fn new_data_key() -> [u8; 32] {
    let mut k = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut k);
    k
}

/// Encrypt one JSON leaf under a data key.
pub fn encrypt_leaf(key_id: &str, data_key: &[u8], leaf: &Value) -> Result<String> {
    let cipher =
        Aes256Gcm::new_from_slice(data_key).map_err(|_| Error::Crypto("bad data key".into()))?;
    let plain = serde_json::to_vec(leaf)?;
    let sealed = seal(&cipher, &plain, key_id.as_bytes())?;
    Ok(format!(
        "{CIPHERTEXT_PREFIX}{key_id}:{}",
        STANDARD.encode(sealed)
    ))
}

/// The key id a ciphertext leaf was written under, if it is one.
pub fn ciphertext_key_id(v: &Value) -> Option<&str> {
    let s = v.as_str()?.strip_prefix(CIPHERTEXT_PREFIX)?;
    s.split_once(':').map(|(id, _)| id)
}

pub fn decrypt_leaf(data_key: &[u8], v: &Value) -> Result<Value> {
    let s = v
        .as_str()
        .and_then(|s| s.strip_prefix(CIPHERTEXT_PREFIX))
        .ok_or_else(|| Error::Crypto("not a ciphertext leaf".into()))?;
    let (key_id, b64) = s
        .split_once(':')
        .ok_or_else(|| Error::Crypto("malformed ciphertext".into()))?;
    let sealed = STANDARD
        .decode(b64)
        .map_err(|_| Error::Crypto("malformed ciphertext".into()))?;
    let cipher =
        Aes256Gcm::new_from_slice(data_key).map_err(|_| Error::Crypto("bad data key".into()))?;
    let plain = open(&cipher, &sealed, key_id.as_bytes())?;
    Ok(serde_json::from_slice(&plain)?)
}

/// generate_token_secret.
pub fn generate_secret() -> String {
    let mut b = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut b);
    URL_SAFE_NO_PAD.encode(b)
}

/// generate_id / generate_token_id.
pub fn generate_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

pub fn hash_secret(secret: &str) -> String {
    let d = Sha256::digest(secret.as_bytes());
    URL_SAFE_NO_PAD.encode(d)
}

/// Constant-time comparison of two strings of possibly different length.
pub fn secret_matches(presented: &str, expected_hash: &str) -> bool {
    let a = hash_secret(presented);
    let (a, b) = (a.as_bytes(), expected_hash.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

type HmacSha256 = Hmac<Sha256>;

/// Normalizer version 1: trim and lowercase.
pub const NORMALIZER_VERSION: i32 = 1;

pub fn normalize_v1(value: &str) -> String {
    value.trim().to_lowercase()
}

/// HMAC-SHA256 blind tags under a root key.
pub struct HmacBlindHasher {
    root: Vec<u8>,
}

impl HmacBlindHasher {
    pub fn new(root: Vec<u8>) -> Self {
        HmacBlindHasher { root }
    }

    fn derived(&self, bc: &str, key: &str) -> Vec<u8> {
        let mut m = <HmacSha256 as Mac>::new_from_slice(&self.root).expect("hmac accepts any key");
        m.update(b"skilj-blind-v1|");
        m.update(bc.as_bytes());
        m.update(b"|");
        m.update(key.as_bytes());
        m.finalize().into_bytes().to_vec()
    }

    /// A one-way value identifying the derived key, so instances configured
    /// with different keys are detected at startup.
    pub fn fingerprint(&self, bc: &str, key: &str) -> String {
        let mut m = <HmacSha256 as Mac>::new_from_slice(&self.derived(bc, key)).expect("hmac");
        m.update(b"fingerprint");
        URL_SAFE_NO_PAD.encode(m.finalize().into_bytes())
    }
}

impl BlindHasher for HmacBlindHasher {
    fn hash(&self, bc: &str, key: &str, value: &str) -> Result<String> {
        let mut m = <HmacSha256 as Mac>::new_from_slice(&self.derived(bc, key)).expect("hmac");
        m.update(normalize_v1(value).as_bytes());
        Ok(format!(
            "blind:{}",
            URL_SAFE_NO_PAD.encode(m.finalize().into_bytes())
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn leaf_round_trip() {
        let k = new_data_key();
        let ct = encrypt_leaf("k1", &k, &json!(42)).unwrap();
        let v = Value::String(ct);
        assert_eq!(ciphertext_key_id(&v), Some("k1"));
        assert_eq!(decrypt_leaf(&k, &v).unwrap(), json!(42));
        assert!(decrypt_leaf(&new_data_key(), &v).is_err());
    }

    #[test]
    fn wrap_round_trip() {
        let m = LocalMasterKey::new("m1", [7u8; 32]);
        let k = new_data_key();
        let w = m.wrap(&k).unwrap();
        assert_eq!(m.unwrap("m1", &w).unwrap(), k.to_vec());
    }

    #[test]
    fn secrets() {
        let s = generate_secret();
        let h = hash_secret(&s);
        assert!(secret_matches(&s, &h));
        assert!(!secret_matches("nope", &h));
    }

    #[test]
    fn blind_normalizes() {
        let h = HmacBlindHasher::new(b"root".to_vec());
        assert_eq!(
            h.hash("b", "email", " A@x.nl").unwrap(),
            h.hash("b", "email", "a@x.nl").unwrap()
        );
        assert_ne!(
            h.hash("b", "email", "a@x.nl").unwrap(),
            h.hash("c", "email", "a@x.nl").unwrap()
        );
    }
}
