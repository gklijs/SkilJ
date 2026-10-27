//! [`VaultTransitProvider`]: the master key in HashiCorp Vault's or
//! OpenBao's Transit secrets engine (docs/architecture.md §230). The key
//! never leaves Vault: skilj sends a data key to be encrypted and gets the
//! ciphertext back, sends the ciphertext to get the data key back, and a
//! rotation's rewrap happens inside Vault without the data key ever
//! coming out. Retiring raises the key's `min_decryption_version`, so
//! Vault itself refuses the old versions.
//!
//! Setting it up:
//!
//! ```text
//! vault secrets enable transit
//! vault write -f transit/keys/skilj
//! ```
//!
//! and a token whose policy allows `update` on `transit/encrypt/skilj`,
//! `transit/decrypt/skilj` and `transit/rewrap/skilj`, `read` on
//! `transit/keys/skilj`, and - for rotating from skilj -
//! `update` on `transit/keys/skilj/rotate` and `transit/keys/skilj/config`.
//! Bounded contexts given keys of their own (docs/architecture.md §230)
//! need the same on each of those keys - a path glob like
//! `transit/+/skilj-*` - plus `create` and `delete` on `transit/keys/<key>`
//! when skilj creates and deletes them.

use super::provider::{BoxFuture, KeyProvider, KeyVersions, WrappedDataKey};
use super::Error;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

/// How the provider authenticates: a token, or a file a Vault Agent keeps
/// a fresh token in, read again for every request.
#[derive(Clone)]
enum Token {
    Static(String),
    File(PathBuf),
}

/// A [`KeyProvider`] backed by a Vault or OpenBao Transit key (see the
/// module doc).
#[derive(Clone)]
pub struct VaultTransitProvider {
    client: reqwest::Client,
    address: String,
    mount: String,
    key: String,
    token: Token,
    namespace: Option<String>,
    name: String,
}

impl std::fmt::Debug for VaultTransitProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultTransitProvider")
            .field("address", &self.address)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl VaultTransitProvider {
    /// The Transit key `key` at `address` (`https://vault.example:8200`),
    /// on the `transit` mount, authenticating with `token`.
    pub fn new(
        address: impl Into<String>,
        key: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self::with_token(address.into(), key.into(), Token::Static(token.into()))
    }

    /// [`new`](Self::new), with the address, token and namespace from
    /// `VAULT_ADDR`, `VAULT_TOKEN` (or `VAULT_TOKEN_FILE`, a file a Vault
    /// Agent keeps a token in) and `VAULT_NAMESPACE`, as Vault's own CLI
    /// reads them.
    pub fn from_env(key: impl Into<String>) -> Result<Self, Error> {
        let missing = |what: &str| Error::ProviderRefused {
            provider: "vault-transit".into(),
            message: format!("{what} is not set"),
        };
        let address = std::env::var("VAULT_ADDR").map_err(|_| missing("VAULT_ADDR"))?;
        let token = match std::env::var("VAULT_TOKEN_FILE") {
            Ok(path) => Token::File(path.into()),
            Err(_) => Token::Static(
                std::env::var("VAULT_TOKEN")
                    .map_err(|_| missing("VAULT_TOKEN or VAULT_TOKEN_FILE"))?,
            ),
        };
        let mut provider = Self::with_token(address, key.into(), token);
        provider.namespace = std::env::var("VAULT_NAMESPACE").ok();
        Ok(provider)
    }

    fn with_token(address: String, key: String, token: Token) -> Self {
        let mut provider = Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("a reqwest client with a timeout always builds"),
            address: address.trim_end_matches('/').to_string(),
            mount: "transit".to_string(),
            key,
            token,
            namespace: None,
            name: String::new(),
        };
        provider.rename();
        provider
    }

    fn rename(&mut self) {
        self.name = format!("vault-transit:{}/{}", self.mount, self.key);
    }

    /// The Transit engine's mount path, `transit` by default.
    pub fn mount(mut self, mount: impl Into<String>) -> Self {
        self.mount = mount.into().trim_matches('/').to_string();
        self.rename();
        self
    }

    /// Reads the token from `path` for every request instead - a Vault
    /// Agent sink, which renews it.
    pub fn token_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.token = Token::File(path.into());
        self
    }

    /// The Vault Enterprise / OpenBao namespace.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// The HTTP client, for TLS settings of your own (a private CA, say).
    pub fn client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    fn unavailable(&self, message: impl std::fmt::Display) -> Error {
        Error::ProviderUnavailable {
            provider: self.name.clone(),
            message: message.to_string(),
        }
    }

    fn refused(&self, message: impl std::fmt::Display) -> Error {
        Error::ProviderRefused {
            provider: self.name.clone(),
            message: message.to_string(),
        }
    }

    fn token(&self) -> Result<String, Error> {
        match &self.token {
            Token::Static(token) => Ok(token.clone()),
            Token::File(path) => std::fs::read_to_string(path)
                .map(|t| t.trim().to_string())
                .map_err(|e| self.unavailable(format!("reading {}: {e}", path.display()))),
        }
    }

    /// One Transit call: `body` posted (or a `GET` without one) to
    /// `/v1/<mount>/<path>`, answering the response's `data`.
    async fn call(&self, path: &str, body: Option<Value>) -> Result<Value, Error> {
        let method = if body.is_some() {
            reqwest::Method::POST
        } else {
            reqwest::Method::GET
        };
        self.request(method, path, body).await
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, Error> {
        self.request_with_status(method, path, body)
            .await
            .map_err(|(_, err)| err)
    }

    /// One request answering its HTTP status and parsed body whatever the
    /// status, except a 5xx, a 429 or no answer, which are
    /// [`Error::ProviderUnavailable`]: for a caller that reads a refusal's
    /// body, as `rewrap` does a batch's.
    async fn request_with_body(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), Error> {
        let url = format!("{}/v1/{}/{}", self.address, self.mount, path);
        let mut request = self.client.request(method, &url);
        if let Some(body) = &body {
            request = request.json(body);
        }
        let mut request = request.header("X-Vault-Token", self.token()?);
        if let Some(namespace) = &self.namespace {
            request = request.header("X-Vault-Namespace", namespace);
        }
        let response = request
            .send()
            .await
            .map_err(|e| self.unavailable(format!("{path}: {e}")))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| self.unavailable(format!("{path}: {e}")))?;
        if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(self.unavailable(format!("{path}: {status} {}", errors(&text))));
        }
        let body = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text)
                .map_err(|e| self.refused(format!("{path}: {status}, not JSON: {e}")))?
        };
        Ok((status.as_u16(), body))
    }

    /// [`request`](Self::request), with the HTTP status of a refusal kept
    /// next to the error - `None` when there was no answer - so a caller can
    /// tell "the key refused this" from "Vault refused the request".
    async fn request_with_status(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, (Option<u16>, Error)> {
        let fail = |status: Option<u16>| move |err: Error| (status, err);
        let url = format!("{}/v1/{}/{}", self.address, self.mount, path);
        let mut request = self.client.request(method, &url);
        if let Some(body) = &body {
            request = request.json(body);
        }
        let mut request = request.header("X-Vault-Token", self.token().map_err(fail(None))?);
        if let Some(namespace) = &self.namespace {
            request = request.header("X-Vault-Namespace", namespace);
        }
        let response = request
            .send()
            .await
            .map_err(|e| (None, self.unavailable(format!("{path}: {e}"))))?;
        let status = response.status();
        let code = Some(status.as_u16());
        let text = response
            .text()
            .await
            .map_err(|e| (None, self.unavailable(format!("{path}: {e}"))))?;
        if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err((
                code,
                self.unavailable(format!("{path}: {status} {}", errors(&text))),
            ));
        }
        if !status.is_success() {
            return Err((
                code,
                self.refused(format!("{path}: {status} {}", errors(&text))),
            ));
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        let value: Value = serde_json::from_str(&text)
            .map_err(|e| (code, self.refused(format!("{path}: not JSON: {e}"))))?;
        Ok(value.get("data").cloned().unwrap_or(Value::Null))
    }

    fn wrapped_from(&self, ciphertext: &str) -> Result<WrappedDataKey, Error> {
        Ok(WrappedDataKey {
            version: version_of(ciphertext).ok_or_else(|| {
                self.refused(format!("unexpected ciphertext {ciphertext:.12}..."))
            })?,
            ciphertext: ciphertext.as_bytes().to_vec(),
        })
    }
}

/// The version in a Transit ciphertext, `vault:v<n>:...`.
fn version_of(ciphertext: &str) -> Option<u32> {
    let rest = ciphertext.strip_prefix("vault:v")?;
    rest.split(':').next()?.parse().ok()
}

/// A parsed body's `errors`, or the body.
fn errors_of(body: &Value) -> String {
    body.get("errors")
        .map_or_else(|| body.to_string(), Value::to_string)
}

/// Vault's `errors` array, or the body as it came.
fn errors(text: &str) -> String {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| v.get("errors").cloned())
        .map_or_else(|| text.trim().to_string(), |e| e.to_string())
}

fn str_field<'v>(value: &'v Value, field: &str) -> Option<&'v str> {
    value.get(field).and_then(Value::as_str)
}

impl KeyProvider for VaultTransitProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn wrap<'a>(&'a self, data_key: &'a [u8; 32]) -> BoxFuture<'a, Result<WrappedDataKey, Error>> {
        Box::pin(async move {
            let data = self
                .call(
                    &format!("encrypt/{}", self.key),
                    Some(json!({ "plaintext": crate::base64::encode(data_key) })),
                )
                .await?;
            let ciphertext = str_field(&data, "ciphertext")
                .ok_or_else(|| self.refused("encrypt answered no ciphertext"))?;
            self.wrapped_from(ciphertext)
        })
    }

    /// Transit's answers are told apart: a 400 or 404 is this key refusing
    /// this ciphertext - wrong key material, a corrupt ciphertext, or a key
    /// that doesn't exist at this Vault - which is
    /// [`Error::DecryptFailed`], or [`Error::KeyVersionRetired`] for a
    /// version below `min_decryption_version`; a 403 is the token's
    /// permissions, and no verdict on the data key
    /// ([`Error::ProviderRefused`]); 5xx, 429 and no answer are
    /// [`Error::ProviderUnavailable`] (docs/architecture.md §230).
    fn unwrap<'a>(&'a self, wrapped: &'a WrappedDataKey) -> BoxFuture<'a, Result<[u8; 32], Error>> {
        Box::pin(async move {
            let ciphertext =
                std::str::from_utf8(&wrapped.ciphertext).map_err(|_| Error::DecryptFailed)?;
            let data = match self
                .request_with_status(
                    reqwest::Method::POST,
                    &format!("decrypt/{}", self.key),
                    Some(json!({ "ciphertext": ciphertext })),
                )
                .await
            {
                Ok(data) => data,
                Err((Some(400 | 404), err)) => {
                    let retired = matches!(&err, Error::ProviderRefused { message, .. }
                        if message.contains("disallowed by policy") || message.contains("too old"));
                    return Err(if retired {
                        Error::KeyVersionRetired {
                            version: wrapped.version,
                        }
                    } else {
                        Error::DecryptFailed
                    });
                }
                Err((_, err)) => return Err(err),
            };
            let plaintext = str_field(&data, "plaintext")
                .and_then(crate::base64::decode)
                .ok_or_else(|| self.refused("decrypt answered no plaintext"))?;
            plaintext.try_into().map_err(|_| Error::DecryptFailed)
        })
    }

    /// Transit's batch `rewrap`, the data keys never leaving Vault. Vault
    /// answers a batch in which an item failed with HTTP 400 - unless
    /// `partial_failure_response_code` (Vault 1.14+, OpenBao) is set and some
    /// item succeeded - so a 400 still carries `batch_results`, read item by
    /// item: one bad ciphertext must not fail the batch, or a context's
    /// rewrap would stall on it forever. A failed item is the key refusing
    /// that ciphertext ([`Error::DecryptFailed`], or
    /// [`Error::KeyVersionRetired`] for "too old").
    fn rewrap<'a>(
        &'a self,
        wrapped: &'a [WrappedDataKey],
    ) -> BoxFuture<'a, Result<Vec<Result<WrappedDataKey, Error>>, Error>> {
        Box::pin(async move {
            if wrapped.is_empty() {
                return Ok(Vec::new());
            }
            let batch: Vec<Value> = wrapped
                .iter()
                .map(|w| json!({ "ciphertext": String::from_utf8_lossy(&w.ciphertext) }))
                .collect();
            let path = format!("rewrap/{}", self.key);
            let body = json!({ "batch_input": batch, "partial_failure_response_code": 200 });
            let data = match self
                .request_with_body(reqwest::Method::POST, &path, Some(body))
                .await?
            {
                (status, body) if status == 200 || status == 400 => {
                    body.get("data").cloned().unwrap_or(Value::Null)
                }
                (status, body) => {
                    return Err(self.refused(format!("{path}: {status} {}", errors_of(&body))))
                }
            };
            let Some(results) = data
                .get("batch_results")
                .and_then(Value::as_array)
                .filter(|r| r.len() == wrapped.len())
            else {
                return Err(self.refused("rewrap answered no batch of the right size"));
            };
            Ok(results
                .iter()
                .zip(wrapped)
                .map(|(result, input)| {
                    let error = str_field(result, "error").filter(|e| !e.is_empty());
                    match (str_field(result, "ciphertext"), error) {
                        (Some(ciphertext), None) => self.wrapped_from(ciphertext),
                        (_, Some(error))
                            if error.contains("too old")
                                || error.contains("disallowed by policy") =>
                        {
                            Err(Error::KeyVersionRetired {
                                version: input.version,
                            })
                        }
                        _ => Err(Error::DecryptFailed),
                    }
                })
                .collect())
        })
    }

    fn versions(&self) -> BoxFuture<'_, Result<KeyVersions, Error>> {
        Box::pin(async move {
            let data = self.call(&format!("keys/{}", self.key), None).await?;
            let number = |field: &str| {
                data.get(field)
                    .and_then(Value::as_u64)
                    .and_then(|n| u32::try_from(n).ok())
                    .ok_or_else(|| self.refused(format!("the key has no {field}")))
            };
            Ok(KeyVersions {
                latest: number("latest_version")?,
                min_decryption: number("min_decryption_version")?,
            })
        })
    }

    fn rotate(&self) -> BoxFuture<'_, Result<u32, Error>> {
        Box::pin(async move {
            self.call(&format!("keys/{}/rotate", self.key), Some(json!({})))
                .await?;
            Ok(self.versions().await?.latest)
        })
    }

    fn retire_below(&self, version: u32) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.call(
                &format!("keys/{}/config", self.key),
                Some(json!({ "min_decryption_version": version })),
            )
            .await?;
            Ok(())
        })
    }

    /// The Transit key `key` on the same mount, with the same token.
    fn key(&self, key: &str) -> Result<std::sync::Arc<dyn KeyProvider>, Error> {
        super::provider::validate_key_name(key)?;
        let mut other = self.clone();
        other.key = key.to_string();
        other.rename();
        Ok(std::sync::Arc::new(other))
    }

    /// `vault write -f transit/keys/<key>` - an AES-256-GCM key - unless
    /// reading the key finds it there already: Vault answers a write to an
    /// existing key with success too, so only the read tells whether this
    /// created it.
    fn create(&self) -> BoxFuture<'_, Result<bool, Error>> {
        Box::pin(async move {
            // Only a 404 says the key isn't there: a 403 - a token that may
            // create but not read - must not be taken for it, or an existing
            // key would be claimed as created.
            match self
                .request_with_status(reqwest::Method::GET, &format!("keys/{}", self.key), None)
                .await
            {
                Ok(_) => return Ok(false),
                Err((Some(404), _)) => {}
                Err((_, err)) => return Err(err),
            }
            self.call(
                &format!("keys/{}", self.key),
                Some(json!({ "type": "aes256-gcm96" })),
            )
            .await?;
            Ok(true)
        })
    }

    /// Allows deletion on the key, then deletes it.
    fn delete(&self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.call(
                &format!("keys/{}/config", self.key),
                Some(json!({ "deletion_allowed": true })),
            )
            .await?;
            self.request(reqwest::Method::DELETE, &format!("keys/{}", self.key), None)
                .await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_version_is_read_from_the_ciphertext() {
        assert_eq!(version_of("vault:v12:abc"), Some(12));
        assert_eq!(version_of("vault:v1:"), Some(1));
        assert_eq!(version_of("plain"), None);
    }

    #[test]
    fn the_name_names_the_key() {
        let provider = VaultTransitProvider::new("http://vault:8200/", "skilj", "t").mount("/kms/");
        assert_eq!(provider.name(), "vault-transit:kms/skilj");
        assert_eq!(provider.address, "http://vault:8200");
    }
}
