//! Blob references - the claim check (issue #132, docs/architecture.md
//! §239). A payload never carries large bytes: it carries a [`BlobRef`],
//! `{"$blob": id, "sha256": ..., "size": ..., "media_type": ...}`, and the
//! bytes live in a [`BlobStore`]. skilj owns the contract and its
//! guarantees, not a storage product: one built-in backend
//! ([`crate::db::PostgresBlobStore`], a table in the bounded context's own
//! schema) and this trait for the rest.
//!
//! Upload first, append second. A blob is stored `pending`
//! ([`crate::db::upload_blob`]); an append whose payload references it
//! checks, under the bounded context's write lock, that the blob exists in
//! this bounded context, is not deleted, and that its size, hash and media
//! type match the reference - and refuses the write otherwise
//! ([`crate::event_store::Error::BlobReferenceInvalid`]). It then records
//! the reference and marks the blob `referenced`. Events are immutable, so
//! a referenced blob stays until erasure or the bounded context's deletion;
//! a blob still `pending` after a grace period is an orphan.
//!
//! A type declares which fields hold blob references ([`BlobField`],
//! next to its sensitive and private fields). A `$blob` value anywhere else
//! in a payload is refused, so a reference can't be smuggled past the
//! checks. `decide()` and projections see the [`BlobRef`] only; resolving
//! the bytes is explicit and lazy ([`crate::db::read_blob`]).
//!
//! Not yet (see the issue's phasing): the REST upload and download routes
//! and their access checks, blobs encrypted under a subject's data key
//! with `forgetSubject` deleting them, and orphan collection. Until
//! encryption lands, a blob field can't also be a sensitive field
//! ([`crate::event_store::Error::InvalidBlobField`]): the reference would
//! be encrypted, and the bytes it points at would not.

use crate::encryption::provider::BoxFuture;
use futures_util::stream::BoxStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The key of a [`BlobRef`]'s id in a payload.
pub const BLOB_KEY: &str = "$blob";

/// The default most bytes one blob may hold: 64 MiB, well under `bytea`'s
/// 1 GB limit. The Postgres backend holds an upload in memory while it
/// hashes and stores it, so this bounds that too.
pub const DEFAULT_MAX_BLOB_BYTES: u64 = 64 * 1024 * 1024;

/// What a payload stores in place of the bytes: which blob, and enough to
/// check that the bytes are the ones the event was about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BlobRef {
    /// The blob's id, random: content addressing would leak equality
    /// across subjects (issue #132's open question, decided for random).
    #[serde(rename = "$blob")]
    pub id: String,
    /// The SHA-256 of the bytes, lowercase hex - plaintext, for integrity.
    pub sha256: String,
    /// The number of bytes.
    pub size: u64,
    /// The media type the uploader gave, e.g. `application/pdf`.
    pub media_type: String,
}

/// A payload field that holds a [`BlobRef`] (or a list of them), declared
/// by an event or command type next to its sensitive and private fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub struct BlobField {
    /// The payload field, dotted for a nested one.
    pub field: crate::shared::FieldPath,
}

impl BlobField {
    /// `field` holds a blob reference.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
        }
    }
}

/// Where a blob is in its life (see this module's doc comment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BlobState {
    /// Uploaded; no event or command references it yet.
    Pending,
    /// At least one stored event or command references it.
    Referenced,
    /// Its bytes are gone; a reference to it is refused.
    Deleted,
}

impl BlobState {
    /// As stored.
    pub fn as_str(self) -> &'static str {
        match self {
            BlobState::Pending => "pending",
            BlobState::Referenced => "referenced",
            BlobState::Deleted => "deleted",
        }
    }

    /// From what [`Self::as_str`] stored; `None` for a state a newer
    /// version wrote.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(BlobState::Pending),
            "referenced" => Some(BlobState::Referenced),
            "deleted" => Some(BlobState::Deleted),
            _ => None,
        }
    }
}

/// What a [`BlobStore`] answers for stored bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StoredBlob {
    /// Where the store keeps them; opaque to skilj.
    pub storage_key: String,
    /// How many bytes it stored.
    pub size: u64,
    /// Their SHA-256, lowercase hex.
    pub sha256: String,
}

impl StoredBlob {
    /// A store's answer for bytes it stored at `storage_key`.
    pub fn new(storage_key: String, size: u64, sha256: String) -> Self {
        Self {
            storage_key,
            size,
            sha256,
        }
    }
}

/// A stream of a blob's bytes, in chunks.
pub type ByteStream<'a> = BoxStream<'a, std::io::Result<Vec<u8>>>;

/// Where blob bytes are kept. It only ever sees opaque bytes, and keys
/// them by bounded context so a bounded context's deletion can find them.
/// `put` streams in, refusing more than `max_bytes`; `get` streams out;
/// `delete` of something already gone is not an error.
pub trait BlobStore: Send + Sync {
    /// Stores `bytes` as blob `id` of `bounded_context`.
    fn put<'a>(
        &'a self,
        bounded_context: &'a str,
        id: &'a str,
        bytes: ByteStream<'a>,
        max_bytes: u64,
    ) -> BoxFuture<'a, crate::error::Result<StoredBlob>>;

    /// The bytes stored at `storage_key`.
    fn get<'a>(
        &'a self,
        bounded_context: &'a str,
        storage_key: &'a str,
    ) -> BoxFuture<'a, crate::error::Result<ByteStream<'static>>>;

    /// Removes the bytes stored at `storage_key`.
    fn delete<'a>(
        &'a self,
        bounded_context: &'a str,
        storage_key: &'a str,
    ) -> BoxFuture<'a, crate::error::Result<()>>;
}

/// SHA-256 over a stream of chunks, counting them, refusing past a limit.
pub struct HashingCounter {
    context: ring::digest::Context,
    size: u64,
    max_bytes: u64,
}

impl HashingCounter {
    /// Counts up to `max_bytes`.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            context: ring::digest::Context::new(&ring::digest::SHA256),
            size: 0,
            max_bytes,
        }
    }

    /// Takes in `chunk`; refuses once the total passes the limit.
    pub fn update(&mut self, chunk: &[u8]) -> crate::error::Result<()> {
        self.size += chunk.len() as u64;
        if self.size > self.max_bytes {
            return Err(crate::event_store::Error::BlobTooLarge {
                limit: self.max_bytes,
            }
            .into());
        }
        self.context.update(chunk);
        Ok(())
    }

    /// The size and lowercase-hex SHA-256 of everything taken in.
    pub fn finish(self) -> (u64, String) {
        let digest = self.context.finish();
        let hex = digest
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        (self.size, hex)
    }
}

/// One `$blob` value found in a payload, with the dotted path of the field
/// holding it - array indices left out, so a list of references is
/// declared by the list's own field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundBlobRef {
    /// The field holding it.
    pub field: String,
    /// The reference, or why it isn't one.
    pub reference: Result<BlobRef, String>,
}

/// Every `$blob` value in `payload`, a JSON document. Cheap when there is
/// none: a payload whose text doesn't contain `"$blob"` isn't parsed.
pub fn find_blob_refs(payload: &str) -> Result<Vec<FoundBlobRef>, serde_json::Error> {
    if !payload.contains("\"$blob\"") {
        return Ok(Vec::new());
    }
    let value: serde_json::Value = serde_json::from_str(payload)?;
    let mut found = Vec::new();
    walk(&value, &mut Vec::new(), &mut found);
    Ok(found)
}

fn walk<'a>(value: &'a serde_json::Value, path: &mut Vec<&'a str>, found: &mut Vec<FoundBlobRef>) {
    match value {
        serde_json::Value::Object(map) if map.contains_key(BLOB_KEY) => {
            found.push(FoundBlobRef {
                field: path.join("."),
                reference: serde_json::from_value::<BlobRef>(value.clone())
                    .map_err(|e| e.to_string()),
            });
        }
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                path.push(key);
                walk(value, path, found);
                path.pop();
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                walk(item, path, found);
            }
        }
        _ => {}
    }
}

/// The references in `payload`, each checked against the type's declared
/// `fields`: a `$blob` in an undeclared field, or one that doesn't parse
/// as a [`BlobRef`], refuses the payload. Pure; the caller checks the
/// references against the stored blobs.
pub fn declared_blob_refs(
    fields: &[BlobField],
    payload: &str,
) -> crate::error::Result<Vec<(String, BlobRef)>> {
    let found = find_blob_refs(payload)
        .map_err(|e| crate::event_store::Error::PayloadDecodeFailed(e.to_string()))?;
    found
        .into_iter()
        .map(|found| {
            if !fields.iter().any(|f| f.field == found.field) {
                return Err(crate::event_store::Error::BlobReferenceInvalid {
                    field: found.field,
                    reason: "this field is not declared as a blob field".to_string(),
                }
                .into());
            }
            match found.reference {
                Ok(reference) => Ok((found.field, reference)),
                Err(reason) => Err(crate::event_store::Error::BlobReferenceInvalid {
                    field: found.field,
                    reason: format!("not a blob reference: {reason}"),
                }
                .into()),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(id: &str) -> serde_json::Value {
        serde_json::json!({"$blob": id, "sha256": "ab", "size": 2, "media_type": "text/plain"})
    }

    #[test]
    fn a_payload_without_blobs_is_not_parsed() {
        assert_eq!(find_blob_refs("not json").unwrap(), Vec::new());
    }

    #[test]
    fn references_are_found_by_field_with_list_indices_left_out() {
        let payload = serde_json::json!({
            "contract": reference("a"),
            "attachments": [reference("b"), reference("c")],
            "nested": {"scan": reference("d")},
            "other": 1,
        })
        .to_string();
        let mut fields: Vec<_> = find_blob_refs(&payload)
            .unwrap()
            .into_iter()
            .map(|f| (f.field, f.reference.unwrap().id))
            .collect();
        fields.sort();
        assert_eq!(
            fields,
            vec![
                ("attachments".to_string(), "b".to_string()),
                ("attachments".to_string(), "c".to_string()),
                ("contract".to_string(), "a".to_string()),
                ("nested.scan".to_string(), "d".to_string()),
            ]
        );
    }

    #[test]
    fn an_undeclared_or_malformed_reference_is_refused() {
        let payload = serde_json::json!({"contract": reference("a")}).to_string();
        assert!(declared_blob_refs(&[BlobField::new("contract")], &payload).is_ok());
        assert!(declared_blob_refs(&[BlobField::new("other")], &payload).is_err());
        let malformed = serde_json::json!({"contract": {"$blob": "a"}}).to_string();
        assert!(declared_blob_refs(&[BlobField::new("contract")], &malformed).is_err());
    }

    #[test]
    fn the_counter_hashes_and_refuses_past_its_limit() {
        let mut counter = HashingCounter::new(5);
        counter.update(b"abc").unwrap();
        let (size, sha) = counter.finish();
        assert_eq!(size, 3);
        assert_eq!(
            sha,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let mut counter = HashingCounter::new(2);
        assert!(counter.update(b"abc").is_err());
    }
}
