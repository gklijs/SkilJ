//! GraphQL identity: a JWT from one trusted IdP, verified against its keys;
//! only the subject claim is trusted.

use std::collections::HashMap;
use std::sync::Arc;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde_json::Value;
use tokio::sync::RwLock;

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing credentials")]
    Missing,
    #[error("invalid token: {0}")]
    Invalid(String),
}

/// Where the IdP's keys come from.
#[derive(Clone)]
pub enum KeySource {
    /// A shared secret (HS256/384/512) - tests and simple setups.
    Secret(Vec<u8>),
    /// A JWKS document given inline.
    Jwks(JwkSet),
    /// A JWKS endpoint, fetched on first use and on unknown key ids.
    JwksUrl(String),
}

/// The one trusted IdP (runtime configuration, not domain state).
#[derive(Clone)]
pub struct JwtConfig {
    pub issuer: String,
    pub audience: String,
    pub algorithm: Algorithm,
    pub subject_claim: String,
    pub keys: KeySource,
}

/// A verified caller.
#[derive(Debug, Clone)]
pub struct Identity {
    pub subject: String,
    /// Seconds since the epoch; a websocket closes then.
    pub expires_at: i64,
}

#[derive(Clone)]
pub struct Verifier {
    config: Arc<JwtConfig>,
    cache: Arc<RwLock<HashMap<String, DecodingKey>>>,
    http: reqwest::Client,
}

impl Verifier {
    pub fn new(config: JwtConfig) -> Self {
        Verifier {
            config: Arc::new(config),
            cache: Arc::new(RwLock::new(HashMap::new())),
            http: reqwest::Client::new(),
        }
    }

    async fn key_for(&self, kid: Option<&str>) -> Result<DecodingKey, AuthError> {
        match &self.config.keys {
            KeySource::Secret(s) => Ok(DecodingKey::from_secret(s)),
            KeySource::Jwks(set) => find(set, kid),
            KeySource::JwksUrl(url) => {
                let k = kid.unwrap_or("").to_string();
                if let Some(key) = self.cache.read().await.get(&k) {
                    return Ok(key.clone());
                }
                let set: JwkSet = self
                    .http
                    .get(url)
                    .send()
                    .await
                    .map_err(|e| AuthError::Invalid(format!("jwks: {e}")))?
                    .json()
                    .await
                    .map_err(|e| AuthError::Invalid(format!("jwks: {e}")))?;
                let key = find(&set, kid)?;
                self.cache.write().await.insert(k, key.clone());
                Ok(key)
            }
        }
    }

    /// Verify a raw JWT.
    pub async fn verify(&self, token: &str) -> Result<Identity, AuthError> {
        let header = decode_header(token).map_err(|e| AuthError::Invalid(e.to_string()))?;
        if header.alg != self.config.algorithm {
            return Err(AuthError::Invalid("unexpected signing algorithm".into()));
        }
        let key = self.key_for(header.kid.as_deref()).await?;
        let mut v = Validation::new(self.config.algorithm);
        v.set_issuer(&[&self.config.issuer]);
        v.set_audience(&[&self.config.audience]);
        v.set_required_spec_claims(&["exp", "iss", "aud"]);
        let data = decode::<HashMap<String, Value>>(token, &key, &v)
            .map_err(|e| AuthError::Invalid(e.to_string()))?;
        let subject = data
            .claims
            .get(&self.config.subject_claim)
            .and_then(Value::as_str)
            .ok_or_else(|| AuthError::Invalid(format!("no '{}' claim", self.config.subject_claim)))?
            .to_string();
        let expires_at = data.claims.get("exp").and_then(Value::as_i64).unwrap_or(0);
        Ok(Identity {
            subject,
            expires_at,
        })
    }

    /// From an `Authorization: Bearer <jwt>` header value.
    pub async fn verify_header(&self, header: Option<&str>) -> Result<Identity, AuthError> {
        let h = header.ok_or(AuthError::Missing)?;
        let t = h.strip_prefix("Bearer ").ok_or(AuthError::Missing)?;
        self.verify(t.trim()).await
    }
}

fn find(set: &JwkSet, kid: Option<&str>) -> Result<DecodingKey, AuthError> {
    let jwk = match kid {
        Some(k) => set.find(k),
        None => set.keys.first(),
    }
    .ok_or_else(|| AuthError::Invalid("unknown signing key".into()))?;
    DecodingKey::from_jwk(jwk).map_err(|e| AuthError::Invalid(e.to_string()))
}
