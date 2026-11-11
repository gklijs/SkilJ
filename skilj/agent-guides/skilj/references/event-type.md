# `EventType`

Pure declaration - an `EventType` has no behaviour to plug in, unlike
`CommandType`/`Projection`. It just describes a payload shape and which
origins may create it.

```rust
pub trait EventType {
    type Payload: Serialize + DeserializeOwned + JsonSchema;

    const NAME: &'static str;
    const BOUNDED_CONTEXT: &'static str = DEFAULT_BOUNDED_CONTEXT; // see registration.md

    fn tag_mappings() -> Vec<TagMapping> { Vec::new() }
    fn owner_tag_key() -> Option<&'static str> { None }
    fn sensitive_fields() -> Vec<SensitiveField> { Vec::new() }
    fn blob_fields() -> Vec<BlobField> { Vec::new() }
    fn private_fields() -> Vec<PrivateField> { Vec::new() }

    fn external_creation_allowed() -> bool { false }
    fn direct_creation_allowed() -> bool { false }
    fn event_read_allowed() -> bool { false }

    fn system_triggered_allowed() -> bool { false }
    fn system_triggered_schedule() -> Option<String> { None }
    fn missed_occurrence_policy() -> Option<MissedOccurrencePolicy> { None }
    fn scheduled_payload() -> Self::Payload { /* panics if not overridden */ }
}
```

Real example - `skilj-demo/src/banking.rs` (`Deposited` in a generated
project's `src/wallet.rs` has the same shape):

```rust
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MoneyDepositedPayload {
    pub account_id: String,
    pub amount: i64,
}

pub struct MoneyDeposited;

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping::new("account", "account_id")]
    }
}
```

## The three creation-origin flags

An `EventType` is created only through an origin it opts into - all
`false` by default:

- `direct_creation_allowed` - a caller with a `DirectCreationToken`
  (REST) or a write-level GraphQL grant creates it directly, no command
  involved.
- `external_creation_allowed` - an external system (a webhook, another
  service) reports it happened, via an `ExternalEventToken`.
- `system_triggered_allowed` - the in-process scheduler fires it on a
  cron schedule (see below). **Command-triggered creation - the
  ordinary case, a `CommandType`'s own `decide()` returning it in
  `CommandDecision::Accepted { events }` - needs none of these flags**;
  it's always permitted.

`event_read_allowed` is unrelated to creation - it opts the type into
being fetchable over REST with an `EventReadToken`.

## Scheduling (`system_triggered_allowed`)

Not a single flag - opting in commits you to two more, both required
together or registration is rejected outright (`MissingScheduleOrPolicy`,
see `common-mistakes.md`):

- `system_triggered_schedule()` - a 7-field Quartz-dialect cron
  expression (the `cron` crate), e.g. `"0 0 * * * *"` for hourly.
- `missed_occurrence_policy()` - `MissedOccurrencePolicy::Skip` or
  `::ReplayBacklog`. There is deliberately no default: both can lose or
  duplicate real work depending on your domain, so you have to say which
  one you mean. `Skip` treats a missed occurrence as gone; `ReplayBacklog`
  catches every missed occurrence up when the process comes back.

When `system_triggered_allowed` is `true`, override `scheduled_payload()`
too - it's called once per eligible occurrence to produce the event's
payload. Leaving it un-overridden panics the first time the scheduler
actually needs it, not at registration time - a genuine programming
error, not something to handle gracefully.

## `tag_mappings` - promoting a payload field to a DCB consistency tag

```rust
fn tag_mappings() -> Vec<TagMapping> {
    vec![TagMapping::new("account", "account_id")]
}
```

`key` is the tag's name (shared across every event/command type that
tags the same conceptual thing - a `CommandType`'s own `tag_mappings`
using the same `key` is what makes `matching_events` scoped correctly,
see `command-type.md`). `field` names which payload field the tag's
*value* comes from at write time - deliberately opt-in and small: only
mapped fields get indexed, to keep the index footprint bounded.

## `sensitive_fields` - opting a payload field into encryption at rest

```rust
fn sensitive_fields() -> Vec<SensitiveField> {
    // field, subject_key, subject_field
    vec![SensitiveField::new("email", "user", "user_id")]
}
```

Writing one needs a key provider on the builder, or the write is refused
with `encryption_master_key_not_configured`: `.key_provider(...)`, in the
`skilj-bootstrap` guide's "Keys the builder needs". A project generated
from skilj-template has none until you add it to `src/bin/server.rs`.

`field` is what gets encrypted (stored as ciphertext, decrypted only for
an entitled reader). `subject_key`/`subject_field` say *whose* data this
is - `subject_field` names the payload field holding the identifier
(`user_id` here) the encryption key is looked up or derived by; that
identifier field itself stays plaintext (it has to, to be findable by).
Adding an entry only affects events created from that point forward -
it never retroactively encrypts history, and there is no backfill path,
so declare this before sensitive data starts flowing through the type.

**A `field` may never also appear in `tag_mappings`** - registration
rejects the overlap (`SensitiveFieldTagOverlap`). Tagging a field is
exactly indexing its plaintext value; encrypting it is the opposite
intent. Tagging the *subject* field instead (e.g. mapping `"account"` to
`account_id` while also naming `account_id` as some other field's
`subject_field`) is fine and normal - only overlap with the sensitive
`field` itself is forbidden.

The one exception is a **blind** mapping,
`TagMapping::blind::<CustomerEmail>("email")`: its tag value is a keyed
hash of the normalized field, never the value, so it may name a
sensitive field. That's how a command checks an email address is unique
without the address being stored in the clear - see the
`skilj-uniqueness` skill and docs/architecture.md §205.

A sensitive field is decrypted for `decide()`, into a temporary copy. A
projection's `project()` only ever sees the ciphertext: see
`projection.md`, "Sensitive state". A command that carries the value
declares the same entry in its own `sensitive_fields()`, since its
payload is stored too.

## `owner_tag_key` - whose record this is

For tenants sharing one bounded context: `Some("company")` names which
of `tag_mappings()`'s keys says who owns the record. A grant or a token
with a scope then reads and writes only records whose owner tag equals
that scope. It must be one of the type's own tag keys
(`InvalidOwnerTagKey` otherwise), and startup never removes one that is
registered. `None`, the default, is a type no scope confines.
`docs/authorization.md` shows where a scope is checked.

## `private_fields` - a field only some readers see

```rust
fn private_fields() -> Vec<PrivateField> {
    vec![PrivateField {
        field: "internal_note".to_string(),
        kind: PrivateFieldKind::Capability,
        capability: Some("staff".to_string()),
        addressee_field: None,
    }]
}
```

Redacted when read, not encrypted: `forgetSubject` doesn't erase it, and
a projection or snapshot folding it keeps it readable in its state.
`kind` says who reads it: `Own` the record's creator, `Capability` a
grant holding `capability`, `Addressed` the party the payload field
`addressee_field` names. Set `capability` or `addressee_field` only for
the kind that uses it. Personal data wants `sensitive_fields`; this is
for a field that part of the audience shouldn't see.

## `blob_fields` - a field holding a reference to large bytes

`fn blob_fields() -> Vec<BlobField>`, on the same field a `BlobRef` typed
payload field has. A `$blob` value in any other field is refused. It can't
overlap a sensitive field: for personal data, use
`BlobField::with_subject(field, subject_key, subject_field)`. See the
`skilj-large-payloads` skill and docs/architecture.md §239.

## `field`/`subject_field` shape rule (applies to `TagMapping` too)

Both are a `FieldPath` (a plain `String`) naming either:

- a bare top-level property (`"account_id"`), or
- a two-segment dotted path reaching one level into a nested shape
  (`"customer.email"`) - the outer segment must be a `$ref`'d nested
  object in the schema, the inner segment one of its own properties.

Either way, what it resolves to must be a scalar or a list-of-scalar
leaf (`string`/`integer`/`number`/`boolean`, or an array of one of
those) - never a nested object or array-of-object itself, and never more
than one level deep. Naming a field the schema doesn't declare, or one
that resolves to something other than a scalar leaf, is rejected at
registration (`InvalidTagMapping`/`InvalidSensitiveField` - see
`common-mistakes.md`).

## Schema versioning

There is no manual `schema_version` field to bump - it's derived
automatically from whether the `Payload` struct's own JSON Schema
changed since the last registration, and re-registering with an
incompatible change (removing a field, tightening optional to required)
is rejected outright rather than silently accepted
(`SchemaIncompatible`). A newly added field must be `Option<T>`.

Additive-only is a deliberate, permanent limitation, not a bug to work
around by force - a genuine reshape (rename, retype, split/merge a
field) is never a legal revision of an existing type. See
`common-mistakes.md`'s `SchemaIncompatible` entry for the real options
(the reasoning is docs/architecture.md §33), including
`skilj_core::plugin::upcast_payload` for interpreting an old payload
differently based on `event.metadata.version` without minting a new
type at all.
