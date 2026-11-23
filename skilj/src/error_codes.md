Every code skilj gives: the `code` of a `skilj-codegen` diagnostic, of a
GraphQL error's `extensions` and of a REST error body, of the startup log
lines that keep a type from registering, and the rejection kinds skilj's own
decisions give (docs/architecture.md §258).

- **A code is stable**; match on it. A message's and a hint's wording is not.
- **Hint and guide come together, or not at all.** A code has them when an
  agent guide section explains it: the hint says in one line what to change,
  the guide names that section. GraphQL puts them in `extensions.hint` and
  `extensions.guide`, REST in the body's `hint` and `guide` (and a batch
  item's), `skilj-codegen` in each diagnostic (`null` when absent), and the
  startup log in the line's `hint` and `guide` fields. A business rejection
  (a `decide()`'s own `rejectionKind`) never has them.
- **A guide is `<skill>` or `<skill>#<anchor>`**: the skill is
  `skilj/agent-guides/<skill>/SKILL.md`, shipped in this crate, and the
  anchor a heading of that file, as GitHub renders it.
- **Rejection kinds skilj itself gives** are a command's `rejectionKind`
  (GraphQL) or `kind` (REST), not an error: a retraction, doubt or lift that
  doesn't fit the event it names, and the default `decide()` and
  `decide_from_snapshot()` of a command type that doesn't override them.
  They have no hint or guide. A `decide()`'s own kinds are the
  application's and aren't listed.
- **Where** says what gives the code: `codegen`, `graphql`, `rest` or
  `startup`. `<entity>_not_found` is a pattern: its instances follow it.
- **A code is never deleted from this table, nor reused.** When nothing
  produces it any more, its row moves to "Retired codes".

`scripts/agent-guides/check.mjs` holds this table to the codes the sources
produce, to `skilj-codegen`'s and `skilj-core`'s tables of hints and guides
(`skilj_core::error::GUIDANCE`), and to the guides.

## Codegen diagnostics

| Code | Where | Hint | Guide |
| --- | --- | --- | --- |
| `invalid_bounded_context_name` | codegen, graphql, rest |  |  |
| `duplicate_generated_item` | codegen |  |  |
| `invalid_blind_tag_name` | codegen |  |  |
| `blind_tag_keep_with_subject` | codegen |  |  |
| `blob_subject_field_not_declared` | codegen |  |  |
| `tag_on_blob_field` | codegen |  |  |
| `invalid_type_name` | codegen |  |  |
| `invalid_field_name` | codegen |  |  |
| `duplicate_field` | codegen |  |  |
| `no_events_for_bounded_context` | codegen |  |  |
| `unknown_field_type` | codegen |  |  |
| `parse` | codegen |  |  |
| `io` | codegen |  |  |
| `generation` | codegen |  |  |
| `drift` | codegen |  |  |
| `usage` | codegen |  |  |
| `unknown_normalizer` | codegen | `set normalize to "email", "compact_uppercase" or the path of a fn(&str) -> String of your own, such as "crate::path::to_fn"` | `skilj-uniqueness#in-a-skiljtoml` |
| `blind_tag_subject_missing` | codegen | `give the erasure = "with_subject" blind tag a subject: the subject_key whose forgetSubject removes the hashes` | `skilj-uniqueness#in-a-skiljtoml` |
| `duplicate_blind_tag_key` | codegen | `declare one [[blind_tag]] per tag key` | `skilj-uniqueness#the-pattern` |
| `blind_subject_tag_is_blind` | codegen | `make the subject_tag a plain tag key, one without a [[blind_tag]] of its own` | `skilj-uniqueness#choosing-the-erasure` |
| `blind_subject_tag_missing` | codegen | `every type with the blind tag must also tag its subject_tag plainly` | `skilj-uniqueness#choosing-the-erasure` |
| `blob_field_not_blob` | codegen | `a blobs entry must name a field of this type declared with type = "blob"` | `skilj-large-payloads#declaring` |
| `sensitive_blob_field` | codegen | `never declare a blob field sensitive; give it a subject in blobs instead` | `skilj-large-payloads#declaring` |
| `tag_field_not_declared` | codegen | `map the tag to a field the type declares in fields (check the spelling), or declare the field` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `sensitive_field_not_declared` | codegen | `name fields the type declares in fields, for both field and subject_field` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `tag_on_sensitive_field` | codegen | `make the tag key a [[blind_tag]] so the value is stored as a keyed hash, not in the clear` | `skilj-uniqueness#first-is-the-value-personal-data` |
| `reserved_event_type_name` | codegen, graphql, rest | `EventRetracted, EventDoubted and DoubtLifted are skilj's built-in event types: rename your type, or remove its declaration and enable retractions/doubts so skilj registers the built-in` | `skilj#when-an-event-was-wrong` |
| `retractable_without_retractions` | codegen | `set retractions = true at the top of the .skilj.toml` | `skilj-uniqueness#in-a-skiljtoml` |
| `retractable_query_latest` | codegen | `remove latest from the retractable command's query items: they read every match` | `skilj-uniqueness#in-a-skiljtoml` |
| `reads_nothing_with_query` | codegen | `keep either reads_nothing = true or the query items, not both` | `skilj#decision-tree` |
| `empty_query_item` | codegen | `give the query item event_types or tags, or set reads_nothing = true for a command that needs no history` | `skilj#decision-tree` |
| `query_latest_zero` | codegen | `set latest to 1 or more, or leave it out to read every match` | `skilj#decision-tree` |
| `query_event_type_not_declared` | codegen | `name an event type this file declares, or a built-in one that retractions or doubts registers` | `skilj#decision-tree` |
| `query_tag_not_declared` | codegen | `name a key of the command type's own tags` | `skilj#decision-tree` |

## GraphQL and REST error codes

| Code | Where | Hint | Guide |
| --- | --- | --- | --- |
| `acknowledgement_regresses` | graphql, rest |  |  |
| `after_command_not_in_bounded_context` | graphql, rest |  |  |
| `batch_too_large` | graphql, rest |  |  |
| `blind_index_key_changed` | graphql, rest | `configure the blind_index_key the stored tags were hashed with; replace a key by rotating it with previous_blind_index_keys` | `skilj-uniqueness#errors` |
| `blind_index_key_not_configured` | graphql, rest | `configure the new blind_index_key and previous_blind_index_keys on every instance; another instance retries meanwhile` | `skilj-uniqueness#errors` |
| `blind_normalizer_changed` | graphql, rest | `revert the normalizer change: NORMALIZER_VERSION has to stay the stored one` | `skilj-uniqueness#errors` |
| `blind_tag_key_in_use` | graphql, rest | `use a new tag key for the blind constraint: this one is already mapped in the clear` | `skilj-uniqueness#errors` |
| `blind_tag_lookup_not_granted` | graphql, rest | `look a blind tag up by plaintext from a grant with can_read_sensitive` | `skilj-uniqueness#errors` |
| `blind_tag_mismatch` | graphql, rest | `use TagMapping::blind::<B> everywhere the key is mapped` | `skilj-uniqueness#errors` |
| `blind_tag_not_declared` | graphql, rest | `declare the BlindTag and blind_index_key on every instance serving the bounded context` | `skilj-uniqueness#errors` |
| `blind_tags_rotated` | graphql, rest | `retry: a key rotation committed while the command was decided` | `skilj-uniqueness#errors` |
| `blob_not_found` | rest | `check that a record references the blob and that its field is visible to you` | `skilj-large-payloads#when-something-is-refused` |
| `blob_reference_invalid` | graphql, rest | `put the reference the upload answered in the payload unchanged, in its own bounded context; the message says what is wrong` | `skilj-large-payloads#when-something-is-refused` |
| `blob_store_error` | graphql, rest |  |  |
| `blob_too_large` | graphql, rest | `raise SkiljBuilder::max_blob_bytes, with an S3 store past what the Postgres store holds comfortably` | `skilj-large-payloads#when-something-is-refused` |
| `blob_upload_not_allowed` | rest | `grant the Role the blob-upload capability (grantCapability)` | `skilj-large-payloads#when-something-is-refused` |
| `bootstrap_secret_mismatch` | graphql, rest |  |  |
| `bootstrap_secret_unavailable` | graphql |  |  |
| `bounded_context_archived` | graphql, rest |  |  |
| `bounded_context_has_no_template` | graphql, rest |  |  |
| `bounded_context_name_taken` | graphql, rest |  |  |
| `bounded_context_not_archived` | graphql, rest |  |  |
| `cannot_archive_admin_context` | graphql, rest |  |  |
| `cannot_delete_admin_context` | graphql, rest |  |  |
| `capability_not_added` | graphql, rest |  |  |
| `capability_not_delegable` | graphql, rest |  |  |
| `capability_not_held` | graphql, rest |  |  |
| `capability_not_revoked` | graphql, rest |  |  |
| `command_proposal_expired` | graphql, rest |  |  |
| `command_proposal_not_found` | graphql, rest |  |  |
| `command_proposal_not_pending` | graphql, rest |  |  |
| `command_proposal_subject_forgotten` | graphql, rest |  |  |
| `command_type_not_in_bounded_context` | graphql, rest |  |  |
| `consumed_event_type_not_in_bounded_context` | graphql, rest |  |  |
| `correlation_id_too_long` | graphql, rest |  |  |
| `cursor_ack_mode_mismatch` | graphql, rest |  |  |
| `database_error` | graphql, rest |  |  |
| `dedupe_and_idempotency_key` | graphql, rest |  |  |
| `direct_creation_not_allowed` | graphql, rest | `the event type doesn't opt into direct creation, which is false by default: override direct_creation_allowed() to return true` | `skilj#decision-tree` |
| `duplicate_active_mapping` | graphql, rest |  |  |
| `duplicate_consumed_event_type` | graphql, rest |  |  |
| `encryption_decrypt_failed` | graphql, rest |  |  |
| `encryption_key_material_changed` | graphql, rest |  |  |
| `encryption_key_not_active` | graphql, rest |  |  |
| `encryption_key_provider_changed` | graphql, rest |  |  |
| `encryption_key_provider_refused` | graphql, rest |  |  |
| `encryption_key_provider_unavailable` | graphql, rest |  |  |
| `encryption_key_version_retired` | graphql, rest |  |  |
| `encryption_legacy_master_key_changed` | graphql, rest |  |  |
| `encryption_legacy_master_key_required` | graphql, rest | `configure .legacy_encryption_master_key(...) until the background task has moved the old data keys to the key provider` | `skilj-bootstrap#keys-the-builder-needs-before-the-first-deploy` |
| `encryption_master_key_not_configured` | graphql, rest | `configure .key_provider(...) on the builder: a type declares sensitive fields` | `skilj-bootstrap#keys-the-builder-needs-before-the-first-deploy` |
| `<entity>_not_found` (`BoundedContext`, `Command`, `CommandType`, `EncryptionKey`, `Event`, `EventType`, `ParkedDelivery`, `PrivateFieldGrant`, `Projection`, `Projection type`, `Role`, `RoleAccessMapping`, `Snapshot`) | graphql |  |  |
| `epoch_changed` | graphql, rest |  |  |
| `error` | graphql |  |  |
| `event_read_not_allowed` | graphql, rest | `declare fn event_read_allowed() -> bool { true } on the event type to read it with an EventReadToken` | `skilj-temporal#getting-the-eventreadtoken` |
| `event_type_not_in_bounded_context` | graphql, rest |  |  |
| `external_creation_not_allowed` | graphql, rest | `the event type doesn't opt into external creation, which is false by default: override external_creation_allowed() to return true` | `skilj#decision-tree` |
| `external_subject_already_claimed` | graphql, rest |  |  |
| `from_sequence_not_committed` | graphql |  |  |
| `grant_bounded_context_mismatch` | graphql, rest |  |  |
| `grant_not_active` | graphql, rest |  |  |
| `grant_scope_mismatch` | graphql, rest |  |  |
| `idempotency_key_too_long` | graphql, rest |  |  |
| `insufficient_access_level` | graphql, rest |  |  |
| `insufficient_approval_role` | graphql, rest |  |  |
| `insufficient_role` | graphql |  |  |
| `invalid_approval_policy` | graphql, rest |  |  |
| `invalid_blind_tag_mapping` | graphql, rest | `tag the owner key plainly, and drop the blind mapping on a scheduled type` | `skilj-uniqueness#errors` |
| `invalid_blob_field` | graphql, rest | `fix the blob field's declaration: never also in sensitive_fields, and a subject can't change or be removed` | `skilj-large-payloads#when-something-is-refused` |
| `invalid_capability_name` | graphql, rest |  |  |
| `invalid_cursor` | graphql |  |  |
| `invalid_filter` | graphql, rest |  |  |
| `invalid_owner_tag_key` | graphql, rest | `map the owner field as a tag first: owner_tag_key() must be one of tag_mappings()'s keys` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `invalid_parked_delivery_request` | graphql, rest |  |  |
| `invalid_payload_limit` | graphql, rest |  |  |
| `invalid_private_field` | graphql, rest | `name a field the schema declares, with capability for Capability and addressee_field for Addressed` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `invalid_request` | rest |  |  |
| `invalid_schema` | graphql, rest | `derive JsonSchema on the payload or state struct, using types schemars can represent` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `invalid_sensitive_field` | graphql, rest | `name scalar leaf fields the payload schema declares (check the spelling), for both field and subject_field` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `invalid_tag_mapping` | graphql, rest | `map the tag to a scalar leaf field the payload schema declares (check the spelling)` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `jwks_fetch_failed` | graphql, rest |  |  |
| `jwt_missing_key_id` | graphql, rest |  |  |
| `jwt_verification_failed` | graphql, rest | `check that .identity_provider names the issuer, audience and JWKS of the tokens being sent` | `skilj-bootstrap#identity_provider-is-a-separate-axis-easy-to-conflate-with-the-above` |
| `live_fold_paused` | graphql, rest | `wait for the rebuild to be promoted, or declare LIVE_FOLD_COMPATIBLE = true when the new fold keeps the live state's schema and meaning` | `skilj#decision-tree` |
| `malformed_credential` | graphql, rest |  |  |
| `malformed_jwt` | graphql, rest |  |  |
| `migration_error` | graphql, rest |  |  |
| `missing_credential` | rest |  |  |
| `missing_schedule_or_policy` | graphql, rest | `override both system_triggered_schedule() and missed_occurrence_policy() on a system-triggered event type` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `missing_subject_claim` | graphql, rest |  |  |
| `no_decider_registered` | graphql, rest | `register the CommandType in the binary that serves requests (#[auto_register] or .command_type::<T>()); another instance may have it` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `no_identity_provider_configured` | graphql | `configure .identity_provider(IdpConfig::new(jwks_url, issuer, audience, algorithm)) so a bearer JWT can be verified` | `skilj-bootstrap#identity_provider-is-a-separate-axis-easy-to-conflate-with-the-above` |
| `no_projection_rebuild_staged` | graphql, rest |  |  |
| `no_read_cursor` | graphql, rest |  |  |
| `not_default_private_reader` | graphql, rest |  |  |
| `not_grantor` | graphql, rest |  |  |
| `not_manual_ack_cursor` | graphql, rest |  |  |
| `not_processed` | graphql, rest |  |  |
| `not_superadmin` | graphql, rest |  |  |
| `ParkedDelivery_retry_in_progress` | graphql |  |  |
| `payload_decode_failed` | graphql, rest |  |  |
| `payload_does_not_match_schema` | graphql, rest | `send a payload that validates against the registered schema; check what schemars renders for Option and enum fields` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `payload_too_large` | graphql, rest | `move the large field to a blob, or raise the payload limit` | `skilj-large-payloads#when-something-is-refused` |
| `private_field_grant_not_active` | graphql, rest |  |  |
| `private_field_not_declared` | graphql, rest |  |  |
| `private_field_overlap` | graphql, rest |  |  |
| `private_field_record_wrong_bounded_context` | graphql, rest |  |  |
| `projection_caught_up_timed_out` | graphql, rest |  |  |
| `projection_not_in_schema` | graphql |  |  |
| `query_too_complex` | graphql |  |  |
| `query_too_expensive` | graphql |  |  |
| `reserved_idempotency_key_prefix` | graphql, rest |  |  |
| `rest_trigger_not_allowed` | graphql, rest |  |  |
| `role_not_active` | graphql, rest |  |  |
| `schema_incompatible` | graphql, rest | `add new fields as Option<T>; never remove, narrow or retype an existing field, and use a new type for a reshape` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `schema_too_new` | graphql, rest |  |  |
| `schema_upgrade_required` | graphql, rest |  |  |
| `secret_access_not_granted` | graphql, rest |  |  |
| `secret_grantee_invalid` | graphql, rest |  |  |
| `secret_name_invalid` | graphql, rest |  |  |
| `secret_not_active` | graphql, rest |  |  |
| `secure_randomness_unavailable` | graphql, rest |  |  |
| `self_approval_not_allowed` | graphql, rest |  |  |
| `self_grant_refused` | graphql, rest |  |  |
| `sensitive_field_tag_overlap` | graphql, rest | `make the tag on the sensitive field blind (TagMapping::blind), or tag the subject field instead` | `skilj-uniqueness#errors` |
| `sensitive_field_without_subject` | graphql, rest |  |  |
| `sensitive_history_unavailable` | graphql, rest | `decide() can't get this event's decrypted copy: its subject was forgotten or its key is unavailable` | `skilj-uniqueness#the-pattern` |
| `snapshot_retracted_meanwhile` | graphql, rest | `retry the command: a retraction reached a snapshot it read` | `skilj-uniqueness#correcting-a-value` |
| `start_at_sequence_mismatch` | graphql, rest |  |  |
| `start_at_time_mismatch` | graphql, rest |  |  |
| `start_at_value_not_allowed` | graphql, rest |  |  |
| `subject_key_reserved` | graphql, rest |  |  |
| `subscription_lagged` | graphql |  |  |
| `superadmin_already_exists` | graphql, rest |  |  |
| `sync_projection_not_declared` | graphql, rest | `keep it retryable: this instance doesn't declare the sync projection, another instance can do the work` | `skilj-temporal#two-independent-pieces---use-either-alone-or-both-together` |
| `tag_mapping_key_dropped` | graphql, rest | `keep every tag key an earlier registration mapped; to stop tagging a field, make a new type` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `template_itself_templated` | graphql, rest |  |  |
| `token_not_active` | graphql, rest |  |  |
| `token_scope_beyond_grant` | graphql, rest |  |  |
| `too_many_subscriptions` | graphql |  |  |
| `too_many_tags` | graphql, rest |  |  |
| `triggered_event_not_in_bounded_context` | graphql, rest |  |  |
| `unauthenticated` | graphql |  |  |
| `unknown_signing_key` | graphql, rest |  |  |
| `unrecognised_credential` | rest |  |  |
| `unrecognised_subject` | graphql, rest |  |  |
| `unregistered_event_type` | graphql, rest | `register an EventType with that NAME in the bounded context: usually a typo, or a missing #[auto_register] impl` | `skilj#if-a-registration-or-a-submission-gets-rejected` |
| `unscoped_admin_required` | graphql, rest |  |  |
| `unsupported` | graphql, rest |  |  |
| `wait_for_sequence_not_committed` | graphql |  |  |
| `wrong_token_variant` | rest |  |  |

## Startup log codes

| Code | Where | Hint | Guide |
| --- | --- | --- | --- |
| `skipped_no_access` | startup | `check that the bounded context exists, that the reconciliation role has an active, unscoped Admin grant on it (for a blind-index key rotation, one on the tenant's template works too), and, for a projection, that every event type it consumes is registered` | `skilj-bootstrap#the-one-thing-every-mistake-here-comes-back-to` |

When reconciliation fails, `build()` logs an error with the failure's own
code from the table above (most often `schema_incompatible`, a refused
registration), and its hint and guide where it has them.

## Rejection kinds skilj itself gives

A command's `rejectionKind` (GraphQL) or `kind` (REST) when skilj's own
check refuses the decision: the retractions, doubts and lifts of
docs/architecture.md §235 and §236, and the defaults of `CommandType::decide`
and `CommandType::decide_from_snapshot`.

| Code | Where | Hint | Guide |
| --- | --- | --- | --- |
| `already_doubted` | graphql, rest |  |  |
| `already_retracted` | graphql, rest |  |  |
| `decide_not_implemented` | graphql, rest |  |  |
| `doubt_of_stance` | graphql, rest |  |  |
| `doubt_target_mismatch` | graphql, rest |  |  |
| `doubt_target_not_found` | graphql, rest |  |  |
| `doubt_target_out_of_scope` | graphql, rest |  |  |
| `doubt_target_retracted` | graphql, rest |  |  |
| `invalid_doubt` | graphql, rest |  |  |
| `invalid_retraction` | graphql, rest |  |  |
| `no_open_doubt` | graphql, rest |  |  |
| `retraction_of_doubt` | graphql, rest |  |  |
| `retraction_of_retraction` | graphql, rest |  |  |
| `retraction_target_mismatch` | graphql, rest |  |  |
| `retraction_target_not_found` | graphql, rest |  |  |
| `retraction_target_out_of_scope` | graphql, rest |  |  |
| `snapshot_misconfigured` | graphql, rest |  |  |

## Retired codes

Codes nothing produces any more. None yet.

| Code | Where | Hint | Guide |
| --- | --- | --- | --- |
