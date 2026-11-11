---
name: skilj-large-payloads
description: >
  Handle a file, document, image or any value too large for a skilj
  payload - decide whether it belongs in the event at all, declare a blob
  field, upload then append, download, encrypt it per subject so
  forgetSubject erases it, offload a marked field in a Kafka/AMQP/NATS
  bridge, and pick limits and a store (Postgres or S3). Triggers on "store
  a PDF/image/attachment/document", "file upload", "large payload", "fat
  event", "BlobRef", "blob_fields", "BlobField", "$blob", "skilj-blob-s3",
  "setPayloadLimits", "payload_limits", "payload_too_large",
  "blob_reference_invalid", "blob_too_large", "invalid_blob_field",
  "blob_not_found", "blob_upload_not_allowed", "blob-upload", a large
  warning in the log, or a bridge logging that a message is too large to
  store. Do NOT use for: adding an ordinary EventType/CommandType with no
  large value (the `skilj` skill), designing the domain's events from
  scratch (skilj-event-modeling first), or a small sensitive field, which
  is `sensitive_fields` (the `skilj` skill).
---

# Large values in skilj

A payload records a fact, and is read back whole by every consistency query
and replay that matches it. The limit is 256 KiB by default, with a warning
above 64 KiB. docs/large-payloads.md has the worked example and
docs/architecture.md §238 and §239 the reasoning; this is what to decide.

## First: does it belong here at all?

Ask whether the event is storing a document instead of a fact about it.
`ContractSigned` with a contract id, signer and date may be enough, with the
document in the system that already keeps documents. Use a blob when the
bytes have to share the event's guarantees: they can't change under it,
can't be missing, follow its access rules, and go when the subject is
forgotten. A URL in a payload gives none of those.

## Declaring

```rust
struct FiledPayload { customer_id: String, document: skilj_core::blob::BlobRef }

fn blob_fields() -> Vec<BlobField> {
    // Personal data: encrypted under the customer's key, erased with them.
    vec![BlobField::with_subject("document", "customer", "customer_id")]
}
```

- **Subject or not:** does forgetting someone have to erase these bytes? Then
  `with_subject`, on the blob field. Otherwise `BlobField::new`, which stores
  them as they came; a bucket then holds them readable.
- **Never** declare the same field in `sensitive_fields`, or a parent or child
  of it: `invalid_blob_field`.
- **Private is fine:** a blob field in `private_fields` redacts its reference,
  and refuses the download with it.
- **A subject can't change or be removed later.** One can be added to a field
  without one; earlier references stay plaintext.
- Registered like `sensitive_fields`: `blobFields` over GraphQL,
  `type = "blob"` and `blobs = [...]` in `.skilj.toml`.

## Upload first, append second

`POST /v1/blobs/{boundedContext}` with a `Content-Type`, plus
`?subjectKey=&subjectValue=` for a subject, answers a reference. Put it in the
payload unchanged. An upload never appended is removed after 24 hours.
`decide()` and projections only ever see the reference.

Who can upload: a writing token whose type declares a blob field (for that
subject's namespace), or a Role whose grant holds the capability `blob-upload`.
A superadmin isn't exempt.

Who can download (`GET /v1/blobs/{boundedContext}/{id}`): whoever can read a
record referencing it with that field visible. An event read token never
decrypts, so a blob with a subject needs a Role with `can_read_sensitive`, or
the subject. A blob nothing references yet is nobody's to read, not even its
uploader's.

## Bridges

`InboundMapping::blobs` names the message fields to upload. They are always
uploaded, whatever their size, so the field has one shape: the type's schema
declares a `BlobRef` there. A base64 string is stored as that text, undecoded.
Messages must be JSON.

A message still over the limit is **not parked**: the bridge logs its
identifier, counts it in `skilj.bridge.oversized_messages`, and moves past it.
That message isn't in skilj anywhere. To recover it, raise the limit and resend
it from the broker.

## When something is refused

| Code | Means | Do |
|---|---|---|
| `payload_too_large` (413) | Over the limit, measured as stored | Move the large field to a blob, or raise the limit |
| `blob_too_large` (413) | Over `max_blob_bytes` | `SkiljBuilder::max_blob_bytes`, with S3 past the Postgres store's comfort |
| `blob_reference_invalid` (400) | Undeclared field, not a reference, or the blob is missing, deleted or differs | The message says which. Don't edit a reference. A reference is valid in its own bounded context only, so a cross-context route must leave it out |
| `invalid_blob_field` (400) | The declaration can't hold | See above |
| `blob_not_found` (404) | Doesn't exist, or you may not read it | The same answer on purpose. Check something references it and the field is visible to you |
| `blob_upload_not_allowed` (403) | The Role's grant lacks `blob-upload` | `grantCapability` |

## Limits and stores

`SkiljBuilder::payload_limits`, and superadmin-only `setPayloadLimits` per
bounded context; `blob_orphan_grace`; `max_blob_bytes`, 64 MiB by default,
which the Postgres store holds in memory during an upload;
`blob_store(Arc::new(skilj_blob_s3::S3BlobStore::from_env("bucket", "prefix")?))`.
