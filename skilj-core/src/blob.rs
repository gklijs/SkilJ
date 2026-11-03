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
//! A blob field with a subject ([`BlobField::with_subject`]) has its bytes
//! encrypted under that subject's data key, so `forgetSubject` erases them
//! with the rest. A blob field can't overlap a sensitive field
//! ([`crate::event_store::Error::InvalidBlobField`]): the reference would
//! be encrypted in place, out of reach of the checks above.

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

/// The default time an uploaded blob nothing references survives: 24 hours,
/// enough for a client to upload, fail and retry its append after an outage.
pub const DEFAULT_ORPHAN_GRACE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

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
    /// The namespace of the subject whose key encrypts the bytes, e.g.
    /// `customer`. Set together with `subject_field`, or not at all: a
    /// blob without a subject is stored as it came.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_key: Option<String>,
    /// The payload field holding the subject's identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_field: Option<crate::shared::FieldPath>,
}

impl BlobField {
    /// `field` holds a reference to a blob with no subject, stored as it
    /// came.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            subject_key: None,
            subject_field: None,
        }
    }

    /// `field` holds a reference to a blob encrypted under the key of the
    /// subject named by `subject_field`'s value in the `subject_key`
    /// namespace, like [`crate::shared::SensitiveField`]: `forgetSubject`
    /// makes the bytes unreadable and removes them.
    pub fn with_subject(
        field: impl Into<String>,
        subject_key: impl Into<String>,
        subject_field: impl Into<String>,
    ) -> Self {
        Self {
            field: field.into(),
            subject_key: Some(subject_key.into()),
            subject_field: Some(subject_field.into()),
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

/// Whether `payload`'s text could hold a `$blob` key at all. A payload may
/// be stored as it arrived, so the key can be spelled with a JSON escape
/// (`"\\u0024blob"`); any `\\u` therefore counts. No other escape can spell
/// one of its characters.
fn may_hold_blob_key(payload: &str) -> bool {
    payload.contains("\"$blob\"") || payload.contains("\\u")
}

/// Every `$blob` value in `payload`, a JSON document. Cheap when there is
/// none: a payload whose text can't hold the key isn't parsed.
pub fn find_blob_refs(payload: &str) -> Result<Vec<FoundBlobRef>, serde_json::Error> {
    if !may_hold_blob_key(payload) {
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

/// One reference in a payload, in a field its type declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredBlobRef {
    /// The field holding it.
    pub field: String,
    /// The reference.
    pub reference: BlobRef,
    /// The `(subject_key, subject_value)` the field declares and the
    /// payload names, for a field with a subject.
    pub subject: Option<(String, String)>,
}

/// The references in `payload`, each checked against the type's declared
/// `fields`: a `$blob` in an undeclared field, one that doesn't parse as a
/// [`BlobRef`], or one in a field whose subject the payload doesn't name
/// refuses the payload. Pure; the caller checks the references against the
/// stored blobs.
pub fn declared_blob_refs(
    fields: &[BlobField],
    payload: &str,
) -> crate::error::Result<Vec<DeclaredBlobRef>> {
    let refuse = |field: String, reason: String| -> crate::error::Error {
        crate::event_store::Error::BlobReferenceInvalid { field, reason }.into()
    };
    let found = find_blob_refs(payload)
        .map_err(|e| crate::event_store::Error::PayloadDecodeFailed(e.to_string()))?;
    let mut declared = Vec::with_capacity(found.len());
    for found in found {
        let Some(blob_field) = fields.iter().find(|f| f.field == found.field) else {
            return Err(refuse(
                found.field,
                "this field is not declared as a blob field".to_string(),
            ));
        };
        let reference = match found.reference {
            Ok(reference) => reference,
            Err(reason) => {
                return Err(refuse(
                    found.field,
                    format!("not a blob reference: {reason}"),
                ))
            }
        };
        let subject = match (&blob_field.subject_key, &blob_field.subject_field) {
            (Some(subject_key), Some(subject_field)) => {
                let value = crate::event_store::payload_subject_value(payload, subject_field)?;
                let Some(value) = value else {
                    return Err(refuse(
                        found.field,
                        format!("the payload names no subject in {subject_field:?}"),
                    ));
                };
                Some((subject_key.clone(), value))
            }
            _ => None,
        };
        declared.push(DeclaredBlobRef {
            field: found.field,
            reference,
            subject,
        });
    }
    Ok(declared)
}

/// Whether two dotted paths overlap: the same field, or one nested in the
/// other.
fn paths_overlap(field_a: &str, field_b: &str) -> bool {
    field_a == field_b
        || field_a
            .strip_prefix(field_b)
            .is_some_and(|rest| rest.starts_with('.'))
        || field_b
            .strip_prefix(field_a)
            .is_some_and(|rest| rest.starts_with('.'))
}

/// Whether a JSON Schema `node` has `path` - following `$ref`, the
/// `anyOf`/`oneOf`/`allOf` an optional field derives to, and a list's
/// `items`, since a list of references is declared by its own field.
fn schema_has_path(root: &serde_json::Value, node: &serde_json::Value, path: &[&str]) -> bool {
    let Some((head, rest)) = path.split_first() else {
        return true;
    };
    schema_alternatives(root, node, 0).into_iter().any(|node| {
        node.get("properties")
            .and_then(|properties| properties.get(*head))
            .is_some_and(|next| schema_has_path(root, next, rest))
    })
}

/// `node` and everything it stands for, a bounded number of references
/// deep so a recursive schema can't loop.
fn schema_alternatives<'a>(
    root: &'a serde_json::Value,
    node: &'a serde_json::Value,
    depth: usize,
) -> Vec<&'a serde_json::Value> {
    let mut out = vec![node];
    if depth >= 8 {
        return out;
    }
    if let Some(target) = node
        .get("$ref")
        .and_then(|r| r.as_str())
        .and_then(|r| r.strip_prefix('#'))
        .and_then(|pointer| root.pointer(pointer))
    {
        out.extend(schema_alternatives(root, target, depth + 1));
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        if let Some(options) = node.get(key).and_then(|o| o.as_array()) {
            for option in options {
                out.extend(schema_alternatives(root, option, depth + 1));
            }
        }
    }
    if let Some(items) = node.get("items") {
        out.extend(schema_alternatives(root, items, depth + 1));
    }
    out
}

/// What registration and `build()` refuse about a type's blob fields: an
/// empty field, one declared twice, one the schema doesn't have, half a
/// subject, a subject whose field the schema doesn't have or whose
/// namespace is reserved, or a field overlapping a sensitive field. The
/// last one would encrypt the reference in place, hiding it from the
/// checks an append makes, so a blob with a subject says so on the blob
/// field itself.
pub fn validate_blob_fields(
    schema: &str,
    blob_fields: &[BlobField],
    sensitive_fields: &[crate::shared::SensitiveField],
) -> crate::error::Result<()> {
    if blob_fields.is_empty() {
        return Ok(());
    }
    let refuse = |why: String| -> crate::error::Result<()> {
        Err(crate::event_store::Error::InvalidBlobField(why).into())
    };
    let Ok(root) = serde_json::from_str::<serde_json::Value>(schema) else {
        return refuse("the schema is not valid JSON".to_string());
    };
    let has = |path: &str| schema_has_path(&root, &root, &path.split('.').collect::<Vec<_>>());
    let mut seen = std::collections::HashSet::new();
    for blob in blob_fields {
        if blob.field.is_empty() {
            return refuse("a blob field names no field".to_string());
        }
        if !seen.insert(blob.field.as_str()) {
            return refuse(format!("blob field {:?} is declared twice", blob.field));
        }
        if !has(&blob.field) {
            return refuse(format!("the schema has no field {:?}", blob.field));
        }
        match (&blob.subject_key, &blob.subject_field) {
            (None, None) => {}
            (Some(subject_key), Some(subject_field)) => {
                if subject_key.is_empty() {
                    return refuse(format!("blob field {:?} has an empty subject", blob.field));
                }
                if crate::secrets::is_reserved_subject_key(subject_key) {
                    return Err(crate::secrets::Error::ReservedSubjectKey.into());
                }
                if !has(subject_field) {
                    return refuse(format!("the schema has no subject field {subject_field:?}"));
                }
            }
            _ => {
                return refuse(format!(
                    "blob field {:?} sets only half of its subject",
                    blob.field
                ))
            }
        }
        if let Some(sensitive) = sensitive_fields
            .iter()
            .find(|s| paths_overlap(&s.field, &blob.field))
        {
            return refuse(format!(
                "blob field {:?} overlaps sensitive field {:?}; declare the subject on the blob field",
                blob.field, sensitive.field
            ));
        }
    }
    Ok(())
}

/// What re-registration refuses: giving a blob field that has a subject
/// another one, or none. The stored blobs and new ones would disagree
/// about whose key protects them, and dropping it would store new bytes in
/// plaintext. Adding a subject to a field without one is allowed: existing
/// references stay plaintext, new uploads are encrypted.
pub fn blob_subjects_unchanged(existing: &[BlobField], new: &[BlobField]) -> bool {
    existing.iter().all(|old| {
        old.subject_key.is_none()
            || new.iter().all(|n| {
                n.field != old.field
                    || (n.subject_key == old.subject_key && n.subject_field == old.subject_field)
            })
    })
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
        let escaped = r#"{"contract":{"\u0024blob":"a","sha256":"ab","size":2,"media_type":"t"}}"#;
        assert_eq!(find_blob_refs(escaped).unwrap().len(), 1);
        assert!(declared_blob_refs(&[BlobField::new("other")], escaped).is_err());
        let malformed = serde_json::json!({"contract": {"$blob": "a"}}).to_string();
        assert!(declared_blob_refs(&[BlobField::new("contract")], &malformed).is_err());
    }

    #[test]
    fn validation_refuses_what_a_type_cannot_mean() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "doc": {"$ref": "#/$defs/Ref"},
                "maybe": {"anyOf": [{"$ref": "#/$defs/Ref"}, {"type": "null"}]},
                "list": {"type": "array", "items": {"$ref": "#/$defs/Ref"}},
                "nested": {"type": "object", "properties": {"doc": {"$ref": "#/$defs/Ref"}}},
            },
            "$defs": {"Ref": {"type": "object", "properties": {"$blob": {"type": "string"}}}},
        })
        .to_string();
        let ok = |fields: Vec<BlobField>| validate_blob_fields(&schema, &fields, &[]).is_ok();
        assert!(ok(vec![
            BlobField::new("doc"),
            BlobField::new("maybe"),
            BlobField::new("list"),
            BlobField::with_subject("nested.doc", "customer", "id"),
        ]));
        assert!(!ok(vec![BlobField::new("")]));
        assert!(!ok(vec![BlobField::new("missing")]));
        assert!(!ok(vec![BlobField::new("doc"), BlobField::new("doc")]));
        assert!(!ok(vec![BlobField::with_subject(
            "doc", "customer", "missing"
        )]));
        let mut half = BlobField::with_subject("doc", "customer", "id");
        half.subject_field = None;
        assert!(!ok(vec![half]));

        // A sensitive field overlapping, the same or nested either way.
        for sensitive in ["doc", "nested", "nested.doc"] {
            let field = if sensitive == "doc" {
                "doc"
            } else {
                "nested.doc"
            };
            let sensitive = crate::shared::SensitiveField::new(sensitive, "customer", "id");
            assert!(validate_blob_fields(&schema, &[BlobField::new(field)], &[sensitive]).is_err());
        }
        // A prefix of the name alone isn't nesting.
        assert!(!paths_overlap("doc", "document"));
    }

    #[test]
    fn a_subject_can_be_added_but_not_changed_or_dropped() {
        let plain = BlobField::new("doc");
        let customer = BlobField::with_subject("doc", "customer", "id");
        let other = BlobField::with_subject("doc", "supplier", "id");
        let unchanged = |existing: &BlobField, new: &BlobField| {
            blob_subjects_unchanged(std::slice::from_ref(existing), std::slice::from_ref(new))
        };
        assert!(unchanged(&plain, &customer));
        assert!(!unchanged(&customer, &plain));
        assert!(!unchanged(&customer, &other));
        assert!(unchanged(&customer, &customer));
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
