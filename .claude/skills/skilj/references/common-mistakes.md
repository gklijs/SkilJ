# Common mistakes and what they actually mean

Every error below is a real `skilj_core::error::Error`/
`skilj_core::event_store::Error` variant - not paraphrased, taken
directly from `skilj-core/src/event_store/mod.rs`/`error.rs`. Each
surfaces as a GraphQL `extensions.code`/REST error body carrying the
same short name (lowercased/snake_cased) shown here.

## At registration time

**`InvalidSchema`** - the derived JSON Schema itself isn't well-formed.
Almost never happens by hand (it's `schemars`-derived, not hand-written)
- if you see this, check the `Payload`/`State` struct actually derives
`JsonSchema` and doesn't use a type `schemars` can't represent.

**`InvalidTagMapping`** / **`InvalidSensitiveField`** - a `TagMapping`/
`SensitiveField`'s `field` (or `subject_field`) names something the
schema doesn't declare, or something that isn't a scalar/list-of-scalar
leaf (a nested object, an array of objects, or more than one level of
dotted nesting). Fix: check the exact field name against the `Payload`
struct's own field names (typos are the usual cause), and check it's
actually a leaf - see `event-type.md`'s "field shape rule" section for
exactly what's allowed.

**`SensitiveFieldTagOverlap`** - the same `field` appears in both
`tag_mappings()` and `sensitive_fields()` on the same type. Tag the
*subject* field instead if you need both an indexed tag and an
encrypted field pointing at the same conceptual subject.

**`SchemaIncompatible`** - re-registering an existing type with a schema
change that would break an existing reader: a field removed, an existing
field tightened from optional to required, or a field's type changed.
Fix: add new fields as `Option<T>`, never remove, narrow, or retype an
existing one. If you need a genuine reshape (rename a field, change its
type, split/merge fields) rather than an additive change, that's not
something a schema revision can express at all - see
[`docs/architecture.md` §33](../../../../docs/architecture.md#payload-upcasting) for the full menu of options (a new,
differently-named type; a custom serde `Deserialize`; reshaping inside
`Projection::project()`; or `skilj_core::plugin::upcast_payload`/
`UpcastStep`, sugar for branching on `event.metadata.version` inside a
hand-written `BoundedContextEvent::try_from_event` to interpret an old
payload differently without a new registered type at all).

Not this error: an *older* version of the application starting after a
newer one registered the type (a restart mid-rollout, or a rollback).
When the stored registration is a valid evolution of the one this
process declares, startup keeps the stored one and lists the type in
`ReconciliationReport::kept_newer` instead of failing. The explicit
`registerEventType`/`registerCommandType` mutations still refuse it.
Likewise startup never *removes* a sensitive field, private field or
owner tag key the stored registration has (reported in
`ReconciliationReport::kept_protections`): deleting one from your type
and redeploying does not turn it off - that takes the explicit
registration mutation.

**`TagMappingKeyDropped`** - re-registering with a `tag_mappings()` that
drops a `key` an earlier registration already used. A tag key, once
live, can't be un-mapped - if you need to stop tagging a field, that's
also effectively a new type.

**`MissingScheduleOrPolicy`** - `EventType::system_triggered_allowed()`
returns `true` but `system_triggered_schedule()`/`missed_occurrence_policy()`
weren't both overridden. See `event-type.md`'s scheduling section - both
are required together, no default for either.

## At submission/creation time (not registration)

**`PayloadDoesNotMatchSchema`** - the JSON payload handed to a command
submission (or a direct/external event creation) doesn't validate
against the registered schema. If this happens for a payload you
believe is correct, check the schema actually registered matches what
you expect - `schemars` output can surprise you for `Option<T>` (renders
as a type array, `["string","null"]`, not a second optional marker) and
for enums (renders via `$ref`, even for a plain unit enum).

**`UnregisteredEventType(name)`** - a `CommandType::decide()` returned
`EventSpec { event_type: "SomeName", .. }` (or a `Projection`'s
`consumed_event_types()` named it) where `"SomeName"` isn't a real,
registered `EventType::NAME` in this bounded context. Usually a typo,
or an event type added to the enum/`decide()` body but never given its
own `#[auto_register(BOUNDED_CONTEXT)] impl EventType for ...` block.

**`NoDeciderRegistered`** - a command submission named a `CommandType`
that's registered in the *database* (so `RegisterCommandType` succeeded
at some point) but this particular running process never registered a
Rust `impl CommandType` for it - the process-local `CommandDispatcher`
has no `decide()` to call. Usually means the binary that's actually
serving requests is missing an `#[auto_register(...)]`'d type (or a
`.command_type::<T>()` call) that a *different* binary/process
registered into the database. Check you're editing the same binary
that's actually running.

## Not an error, but easy to miss

**Nothing registered, no error at all** - a bounded context created but
never granted access to your `reconciliation_role` registers nothing on
any deploy, silently, every time. Check `report.skipped_no_access`
(returned from `.build()`) rather than assuming a clean `Ok` means
everything registered - see `registration.md`.
