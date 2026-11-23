# Validate and preview a skilj model

Install the binary with `cargo install skilj-codegen`, or use
`cargo run -p skilj-codegen --` in this workspace in place of `skilj-codegen`.
The binary ships with the library; existing `build.rs` calls to
`skilj_codegen::generate(&source)` continue to work.

```sh
skilj-codegen check src/banking.skilj.toml
skilj-codegen generate src/banking.skilj.toml
skilj-codegen generate src/banking.skilj.toml --output banking_generated.rs
```

`check` uses the same parser, semantic validator and Rust emitter as `generate`.
It checks that the generated tokens parse as Rust. Generated command decisions
still delegate to handwritten `decide_*` functions; projections also remain
handwritten. Validation does not prove domain correctness, type-check the
output, or compile the complete application. Keep application compilation and
domain tests in your workflow.

For CI, compare against a reviewed file without changing it:

```sh
skilj-codegen generate src/banking.skilj.toml --check banking_generated.rs
```

Comparison is byte-for-byte, including whitespace. Repeated generation from
identical input produces identical output. `--output` and `--check` are mutually
exclusive. Explicit file output writes a temporary file beside the destination,
then replaces the destination after successful generation and writing. Validation
and generation failures leave existing output intact. Shell redirection (`>`)
is managed by the shell and does not provide that protection. Quote paths that
contain spaces.

For coding agents or scripts:

```sh
skilj-codegen check "src/my context.skilj.toml" --diagnostic-format json
```

Successful `check`, file generation and comparison are silent. Generation without
an output option writes only Rust to stdout. Failures write diagnostics only to
stderr. `--help` and `--version` use stdout. JSON mode emits one object on stderr:

```json
{"version":2,"diagnostics":[{"code":"tag_on_sensitive_field","category":"semantic","path":"model.skilj.toml","context":"event_type \"EmailClaimed\"","message":"...","line":null,"column":null,"hint":"make the tag key a [[blind_tag]] so the value is stored as a keyed hash, not in the clear","guide":"skilj-uniqueness#first-is-the-value-personal-data"}]}
```

Version 2 has a `diagnostics` array; each entry has `code`, `category`, `path`,
`context`, `message`, `line`, `column`, `hint` and `guide`. Consumers should
accept additional fields.

- **`code`** is specific and stable: one per rule (`duplicate_field`,
  `tag_on_sensitive_field`, `unknown_field_type`, ...), listed below. Codes and
  exit statuses are the machine interface; message wording, hint wording and
  context text may evolve.
- **`category`** is the coarse kind - `parse`, `semantic`, `io`, `generation`,
  `drift` or `usage` - which is what `code` was in version 1. For the whole-run
  codes `parse`, `io`, `generation`, `drift` and `usage`, code and category are
  the same.
- **`hint` and `guide`** come together or are both `null`. Where an agent guide
  section explains the code, `hint` says in one line what to change and `guide`
  names the section: `<skill>` or `<skill>#<anchor>`, a skill under
  `skilj/agent-guides/` (`skilj-uniqueness#first-is-the-value-personal-data` is
  the heading "First: is the value personal data?" of
  `skilj/agent-guides/skilj-uniqueness/SKILL.md`).

Context names the declaration or query item when available; field details appear
in the message. Parse errors retain one-based line and Unicode character column
when the TOML parser supplies a span, and so does `unknown_field_type`, at the
type's value. Semantic errors have no source spans, so their positions are null.
Multiple semantic problems are returned together. Successful invocations emit no
JSON document.

Text mode prints one line per diagnostic, `<path>: <code>: <message>`
(`<code>: <message>` when there is no path), followed by `  hint: <hint>` and
`  guide: <guide>` lines when the diagnostic has them. An argument error prints
`usage: ` and clap's message.

A field type this format doesn't have, such as `type = "uuid"`, is
`unknown_field_type` (category `parse`) on the field, with the value's position,
instead of a TOML error. Any other shape error in the same file, such as a
misspelled key, is reported next to it as `parse`; a file with only other syntax
or shape errors gets the one `parse` diagnostic as before.

| Exit | Meaning | Diagnostic category |
| --- | --- | --- |
| 0 | Success (including help/version) | None |
| 1 | Malformed TOML, invalid shape, unknown field type, or semantic validation failure | `parse`, `semantic` |
| 2 | Invalid CLI arguments | `usage` |
| 3 | Input/output failure or internal generation failure | `io`, `generation` |
| 4 | Generated source differs from the comparison file | `drift` |

### The codes

The table of every code, with its hint and guide, is
[`skilj/src/error_codes.md`](../skilj/src/error_codes.md) (also the `skilj`
crate's `error_codes` module in its API docs), next to the GraphQL, REST and
startup codes.

- Category `semantic`, one per validation rule: `invalid_bounded_context_name`,
  `duplicate_generated_item`, `invalid_blind_tag_name`, `unknown_normalizer`,
  `blind_tag_keep_with_subject`, `blind_tag_subject_missing`,
  `duplicate_blind_tag_key`, `blind_subject_tag_is_blind`,
  `blind_subject_tag_missing`, `blob_field_not_blob`,
  `blob_subject_field_not_declared`, `sensitive_blob_field`, `tag_on_blob_field`,
  `invalid_type_name`, `invalid_field_name`, `duplicate_field`,
  `tag_field_not_declared`, `sensitive_field_not_declared`,
  `tag_on_sensitive_field`, `reserved_event_type_name`,
  `retractable_without_retractions`, `retractable_query_latest`,
  `reads_nothing_with_query`, `empty_query_item`, `query_latest_zero`,
  `query_event_type_not_declared`, `query_tag_not_declared`, and from `import`
  `no_events_for_bounded_context`.
- Category `parse`: `unknown_field_type`, and `parse` itself.
- The other whole-run codes: `io`, `generation`, `drift`, `usage`.

The library gives the same codes: `skilj_codegen::validate` returns
`ValidationProblem`s with `code`, `hint`, `guide`, `span` and `category()`, and
`skilj_codegen::Error::Invalid` carries them.

Version 1 had `code` set to what is now `category`, and no `category`, `hint` or
`guide`. A consumer of version 1 reads `category` where it read `code`.

A missing comparison file is an I/O failure. Drift never modifies it. Diagnostic
paths identify the input, output, or comparison file responsible for the error;
usage errors use an empty path. Paths are JSON strings (non-Unicode OS paths
are represented lossily). JSON format can be selected before or after the
subcommand, including for argument errors.

## Import a model from an Event Modeling tool

`import` writes one bounded context of a model made with an Event Modeling
tool as a `.skilj.toml` ([Bring your modeling tool](modeling-tools.md) lists
the tools and their naming conventions):

```sh
skilj-codegen import --from emhcl model.json banking
skilj-codegen import --from em-json board-export.json banking --output src/banking.skilj.toml
skilj-codegen import --from evml model.evml banking --output src/banking.skilj.toml
```

`--from` is `emhcl` (`emhcl export` JSON), `em-json` (Event Modeling
Specification JSON) or `evml` (the event-modeling-tools text language). The
`.skilj.toml` goes to stdout, or to `--output`, which is replaced only after a
successful import, as for `generate`. What the import left out or couldn't
decide goes to stderr as `note: ...` lines, or with `--diagnostic-format json`
as one object, `{"version":1,"notes":["..."]}` - the notes document stays
version 1; a successful import without notes writes nothing to stderr. The same
notes head the file as comments.

A model that isn't in the source's format is a `parse` failure (exit 1; an
`.evml` error has its line), a bounded context name that isn't one is
`invalid_bounded_context_name`, a bounded context the model has no events of is
`no_events_for_bounded_context`, an import that wouldn't generate has the
rule's own code (all category `semantic`, exit 1), and a file that can't be
read is `io` (exit 3). The `emhcl_import` example this replaces wrote the
same file.
