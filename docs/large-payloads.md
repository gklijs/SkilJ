# Large payloads

A skilj payload records a fact. It is read back whole by every consistency
query and replay that matches it, held in the event cache, and published as
one message by the bridges, so it has a limit: 256 KiB by default, with a
warning above 64 KiB. Before reaching for a blob, ask whether the event is
storing a document instead of a fact about it: `ContractSigned` with a
contract id, signer and date is usually enough, and the document can live
where documents live.

When the bytes do belong with the fact, use a blob. skilj then owns what a
URL in a payload can't give you: the bytes can't change under the event,
can't be missing, follow the event's access rules, and go when the subject
is forgotten or the bounded context deleted. Design: docs/architecture.md
§238 and §239.

## Declare the field

```rust
#[derive(Serialize, Deserialize, JsonSchema)]
struct ContractSignedPayload {
    customer_id: String,
    document: skilj_core::blob::BlobRef,
}

impl EventType for ContractSigned {
    // ...
    fn blob_fields() -> Vec<BlobField> {
        // Personal data: encrypted under the customer's key, erased with them.
        vec![BlobField::with_subject("document", "customer", "customer_id")]
    }
}
```

Use `BlobField::new("document")` for bytes with no subject, such as a product
manual; those are stored as they came. A list of references is declared by
the list's field. A blob field can be private, but can't overlap a sensitive
field. In `.skilj.toml`:

```toml
fields = [{ name = "customer_id", type = "string" }, { name = "document", type = "blob" }]
blobs = [{ field = "document", subject_key = "customer", subject_field = "customer_id" }]
```

## Upload, then append

```sh
curl -X POST "$SKILJ/v1/blobs/contracts?subjectKey=customer&subjectValue=c1" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/pdf" \
  --data-binary @contract.pdf
# 201 {"$blob":"...","sha256":"...","size":123,"media_type":"application/pdf"}
```

Put that reference in the payload as it is. The append refuses it with
`blob_reference_invalid` if anything differs, or if the subject isn't the one
the payload names. The token is a writing token of a type declaring a blob
field; a Role's JWT needs the `blob-upload` capability on its grant. A blob
nothing references is removed after 24 hours (`blob_orphan_grace`), so an
upload whose append never happens costs nothing lasting.

## Download

```sh
curl "$SKILJ/v1/blobs/contracts/$ID" -H "Authorization: Bearer $TOKEN" -o contract.pdf
```

You can download what you can read: a record referencing the blob, with that
field visible. Anything else is a 404. An event read token can't decrypt, so
a blob with a subject needs a Role with `can_read_sensitive` or the subject
itself. `decide()` and projections only ever see the reference.

## Bridges

```rust
InboundMapping {
    credential,
    action: InboundAction::Record { event_type: "ContractSigned".into() },
    blobs: Some(InboundBlobs {
        bounded_context: "contracts".into(),
        fields: vec![InboundBlobField {
            field: "document".into(),
            media_type: "application/pdf".into(),
            subject: Some(("customer".into(), "customer_id".into())),
        }],
    }),
}
```

The bridge always uploads the marked fields, whatever their size, so the
stored field has one shape. Anything still over the limit is refused: the
bridge logs its identifier, counts it in `skilj.bridge.oversized_messages`,
and moves past it without parking. If you raise the limit, resend it from the
broker. A base64 string is stored as that text; there is no decoding.

## Limits and stores

- `SkiljBuilder::payload_limits`, and `setPayloadLimits` per bounded context.
- `SkiljBuilder::max_blob_bytes`, 64 MiB by default. The Postgres store holds
  an upload in memory and `bytea` stops at 1 GB, so go higher only with S3.
- `SkiljBuilder::blob_store(Arc::new(skilj_blob_s3::S3BlobStore::from_env("bucket", "prefix")?))`.
  Bytes without a subject are readable in the bucket, so give it the access
  control they need.
- A reference is valid in its own bounded context only: a cross-context route
  must leave it out of what it forwards.
