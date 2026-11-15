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
{"version":1,"diagnostics":[{"code":"semantic","path":"model.skilj.toml","context":"event_type \"Changed\"","message":"...","line":null,"column":null}]}
```

Version 1 has a `diagnostics` array; each entry has `code`, `path`, `context`,
`message`, `line`, and `column`. Consumers should accept additional fields.
Codes and exit statuses are the machine interface; message wording and context
text may evolve. Context names the declaration or query item when available;
field details appear in the message. Parse errors retain one-based line and
Unicode character column when the TOML parser supplies a span. Semantic errors
have no source spans, so their positions are null. Multiple semantic problems
are returned together. Successful invocations emit no JSON document.

| Exit | Meaning | Diagnostic code |
| --- | --- | --- |
| 0 | Success (including help/version) | None |
| 1 | Malformed TOML, invalid shape, or semantic validation failure | `parse`, `semantic` |
| 2 | Invalid CLI arguments | `usage` |
| 3 | Input/output failure or internal generation failure | `io`, `generation` |
| 4 | Generated source differs from the comparison file | `drift` |

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
as one object, `{"version":1,"notes":["..."]}`; a successful import without
notes writes nothing to stderr. The same notes head the file as comments.

A model that isn't in the source's format is a `parse` failure (exit 1; an
`.evml` error has its line), a bounded context the model has no events of or
an import that wouldn't generate is `semantic` (exit 1), and a file that can't
be read is `io` (exit 3). The `emhcl_import` example this replaces wrote the
same file.
