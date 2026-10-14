---
name: skilj-uniqueness
description: >
  Enforce that a value is unique within a skilj bounded context - an
  email address, a username, a national id, an IBAN, an order number -
  with a DCB consistency query instead of a unique index, and, when the
  value is personal data, with a blind tag (a keyed hash) so the value is
  never stored in the clear and forgetSubject can free it. Triggers on
  "unique email", "uniqueness constraint", "make sure X is unique", "no
  two customers with the same ...", "already registered/taken", "unique
  username", "blind index", "BlindTag", "TagMapping::blind",
  "blind_index_key", "blind_tag_not_declared", "blind_tag_mismatch",
  "blind_index_key_changed", a sensitive field that also needs to be
  looked up by value, or checking a value before submitting - "is this
  email available", "is this username free", "check if taken before
  registering", "look a customer up by email". Do NOT use for: uniqueness
  across bounded contexts (that needs its own bounded context, see
  below), designing the domain's events from scratch (skilj-event-modeling
  first), or adding an ordinary EventType/CommandType with no uniqueness
  rule (the `skilj` skill).
---

# Unique values in skilj

skilj has no unique index to declare on a payload field. A uniqueness rule
is a decision like any other: the command that claims a value reads the
earlier claims of that value through its consistency query, and `decide()`
rejects when one is still standing. Because the claim and the check are
the same tag, two commands racing for one value conflict on it, and
exactly one wins (`docs/architecture.md` §205; the race is tested in
`skilj/tests/blind_tags.rs`).

## First: is the value personal data?

- **No** (an order number, a SKU, a slug): use a plain tag. Everything
  below applies with `TagMapping::new("order", "order_number")` and no
  `BlindTag`.
- **Yes** (an email address, a phone number, a national id, a username
  that identifies a person): use a **blind tag**. A plain tag stores the
  value in the clear in `events.tags`, `event_tags`,
  `commands.consistency_tags`, the event cache and every surface that
  serves tags - which is why skilj refuses a plain tag on a sensitive
  field (`sensitive_field_tag_overlap`). Don't work around that by
  leaving the field unencrypted or by hashing it yourself in the payload:
  a blind tag is that hash, keyed, normalized, erasable, and checked by
  every instance.

A blind tag's value is `HMAC-SHA256(key, normalize(value))`. The key is
derived per bounded context and tag key from the `blind_index_key`, so
the same address hashes differently in two bounded contexts or under two
constraints, and nobody without the key can test whether an address is
registered.

## The pattern

Read [references/blind-tags.md](references/blind-tags.md) for the full,
annotated version. In short:

1. **Declare the constraint** - one `BlindTag` per tag key, registered
   like any other plugin type:

   ```rust
   use skilj::{auto_register, normalize, BlindErasure, BlindTag};

   pub struct CustomerEmail;

   #[auto_register(BOUNDED_CONTEXT)]
   impl BlindTag for CustomerEmail {
       const TAG_KEY: &'static str = "email";
       // "customer" is both the SensitiveField.subject_key and the plain tag
       // carrying the subject's value; BlindErasure::WithSubject { subject_key,
       // subject_tag } when they differ.
       const ERASURE: BlindErasure = BlindErasure::with_subject("customer");
       fn normalize(value: &str) -> String {
           normalize::email(value)
       }
   }
   ```

2. **Tag it blind** on every type carrying the value, and encrypt the
   field itself as usual:

   ```rust
   fn tag_mappings() -> Vec<TagMapping> {
       vec![
           TagMapping::new("customer", "customer_id"),
           TagMapping::blind::<CustomerEmail>("email"),
       ]
   }
   fn sensitive_fields() -> Vec<SensitiveField> {
       vec![SensitiveField::new("email", "customer", "customer_id")]
   }
   ```

3. **Model claims and releases as events** - `EmailClaimed` and
   `EmailReleased` (or `CustomerRegistered` and `CustomerEmailChanged`
   carrying the old address too). The rule is "the latest claim-or-release
   of this value is a claim":

   ```rust
   fn consistency_query() -> Vec<QueryItemMapping> {
       vec![QueryItemMapping::types(&["EmailClaimed", "EmailReleased"])
           .tagged(vec![TagMapping::blind::<CustomerEmail>("email")])
           .latest()]
   }
   fn decide(payload: &Self::Payload, events: &[Self::Event]) -> CommandDecision {
       if matches!(events.last(), Some(CustomerEvent::Claimed)) {
           return CommandDecision::Rejected { reason: "email taken".into(), kind: "email_taken".into() };
       }
       // accept: emit EmailClaimed
   }
   ```

   `decide()` sees earlier events as stored, so their encrypted email is
   ciphertext. That's fine and intended: the tag already selected exactly
   the events with the same normalized address - decide on *which events
   matched*, never by comparing payload values.

4. **Configure the key** once per process, identical on every instance:

   ```rust
   Skilj::builder(url)
       .auto_register()
       .encryption_master_key(master_key)
       .blind_index_key(BlindIndexKey::from_bytes(blind_key_bytes))
   ```

   See "The key" below before the first deploy.

## In a `.skilj.toml`

If the bounded context's shapes are generated with `skilj-codegen`, declare
the constraint there; every tag on its key becomes `TagMapping::blind`:

```toml
[[blind_tag]]
name = "CustomerEmail"          # the generated BlindTag marker type
key = "email"
normalize = "email"             # or "compact_uppercase", or "crate::path::to_fn"
erasure = "with_subject"        # or "keep"
subject = "customer"            # subject_key, and the subject tag unless subject_tag = "..."

[[event_type]]
name = "EmailClaimed"
fields = [{ name = "customer_id", type = "string" }, { name = "email", type = "string" }]
tags = { customer = "customer_id", email = "email" }
sensitive = [{ field = "email", subject_key = "customer", subject_field = "customer_id" }]
```

The `decide_*` functions stay hand-written. Complete example:
`skilj-demo/tests/customers.skilj.toml` with `skilj-demo/tests/customers.rs`.

## The key

`blind_index_key` is a root secret every constraint's HMAC key is derived
from. Treat it like the encryption master key, and keep it separate:

- **Identical on every instance, stable forever.** Stored hashes only
  match new ones under the same key. skilj stores a fingerprint of each
  constraint's derived key and refuses to start with another key
  (`blind_index_key_changed`, `docs/architecture.md` §208), so a mistyped
  or regenerated key fails loudly instead of letting every value be
  claimed again.
- **Not rotatable in place.** The hashes can't be recomputed: the values
  are encrypted per subject, and forgotten subjects' are gone. Rotating
  means a new tag key and `BlindTag`, re-claiming the live values under
  it (the command that changes them can claim both), and the old key kept
  until that is done. Say so when the user asks about key rotation.
- **Losing it** loses the guarantee for every value claimed so far. Back
  it up with the master key, in the same secret store.
- **Loading it** is the application's: `BlindIndexKey::from_bytes` takes
  `[u8; 32]`, like `EncryptionMasterKey`. Decode it from a secret, e.g.
with the `hex` crate:

  ```rust
  let bytes: [u8; 32] = hex::decode(std::env::var("SKILJ_BLIND_INDEX_KEY")?)?
      .try_into()
      .map_err(|_| "SKILJ_BLIND_INDEX_KEY must be 32 bytes")?;
  builder.blind_index_key(BlindIndexKey::from_bytes(bytes))
  ```

  Generate it once (`openssl rand -hex 32`); never derive it from
  something guessable, since anyone with the key can test any value
  against the stored hashes.

## Choosing the erasure

`forgetSubject` crypto-shreds the subject's payloads either way. The
`BlindErasure` decides what happens to the hash:

- **`WithSubject { subject_key, subject_tag }`** - the hash is removed
  from every event and command whose `subject_tag` tag carries the
  forgotten subject's value. The address can be registered again and no
  trace of it remains. The default choice for personal data under a
  right to erasure. Every type with the blind mapping must also tag
  `subject_tag` plainly; `build()` checks it.
- **`Keep`** - the hash stays, so the value stays taken after the person
  is forgotten. For rules that outlive the person: a free trial used
  once, an address that was banned for abuse. Only someone with the
  constraint's key can test a value against it. Make sure that retention
  is something you can justify - the hash is still pseudonymous personal
  data.

Ask the user which one applies, unless the domain makes it obvious.

## Normalization

`normalize()` decides which spellings are the same value. Ready-made:
`normalize::email` (trim, lowercase - deliberately no Gmail dot or `+`
folding) and `normalize::compact_uppercase` (remove spaces, uppercase -
IBANs, postcodes). Write your own when the domain needs it, and keep it
deterministic. It can also mix in an application secret (a pepper) the
database never sees.

Changing what it returns for any input means bumping `NORMALIZER_VERSION`
- and that is refused at startup (`blind_normalizer_changed`), because
values hashed the old way would no longer match. Get the normalizer right
before the first deploy.

## Limits to tell the user about

- **One bounded context.** The key is per bounded context, so blind tags
  never match across them. If a value must be unique across several, give
  that rule its own bounded context (a "registry" context owning the
  claims) and route to it; don't try to share a tag.
- **A key's blindness is fixed.** A tag key a registered type already maps
  in the clear can't become blind (`blind_tag_key_in_use`) - its values
  are already stored. Introduce a new key for the blind constraint.
- **A blind key is not an owner key** (`owner_tag_key`), can't be on a
  scheduled event type, and can't key a `Snapshot` when it's erased with
  its subject.
- **Looked up by plaintext only with a sensitive grant.** A client can't
  compute the hash, so GraphQL's `TagInput` takes `plaintext: true` and the
  server hashes the value as stored - how a form checks "is this address
  taken?" before submitting the command, or a login finds its customer:
  ```graphql
  queryEvents(boundedContext: "crm", eventTypes: ["EmailClaimed", "EmailReleased"],
    tags: [{ key: "email", value: "alice@example.com", plaintext: true }]) { sequence payload }
  ```
  Taken when the last result is a claim. It needs `can_read_sensitive` on an
  admin-level grant (`blind_tag_lookup_not_granted` otherwise), so it runs
  server-side, not in an end user's browser. Without `plaintext` a value is
  compared as it is - right for a hash read back from an event's tags.
- **Every instance must declare it.** An instance serving the bounded
  context without the `BlindTag` refuses the command with
  `blind_tag_not_declared` (retried by another instance) rather than store
  the value in the clear. Roll out with all instances on a version that
  declares it.

## Errors

| code | cause | fix |
|---|---|---|
| `blind_tag_not_declared` | this process can't hash a blind mapping | declare the `BlindTag` and `blind_index_key` on every instance |
| `blind_tag_mismatch` | a key is blind on one type and plain on another, or blind with no stored `BlindTag` | use `TagMapping::blind::<B>` everywhere the key is mapped |
| `blind_tag_key_in_use` | making blind a key already mapped in the clear | use a new tag key |
| `blind_normalizer_changed` | `NORMALIZER_VERSION` differs from the stored one | revert the normalizer change |
| `blind_index_key_changed` | this process's `blind_index_key` isn't the one the stored tags were hashed with | configure the original key; see "The key" |
| `invalid_blind_tag_mapping` | blind owner key, or blind mapping on a scheduled type | tag the owner plainly; drop the blind mapping there |
| `sensitive_field_tag_overlap` | a *plain* tag on a sensitive field | `TagMapping::blind` |
| `blind_tag_lookup_not_granted` | `plaintext: true` on a blind key from a grant that can't read sensitive fields | use a grant with `can_read_sensitive` |

A `build()` configuration error names the type and key: a `BlindTag`
without `blind_index_key`, a blind mapping without a `BlindTag`, a
declared key mapped plainly, a missing `subject_tag` mapping.

Design and rationale: `docs/architecture.md` §205 (blind tags), §206
(`.skilj.toml` and the API by use case), §207 (plaintext lookup), §208
(the key fingerprint). Worked tests: `skilj/tests/blind_tags.rs`, and
`skilj-demo/tests/customers.rs` for the generated version.
