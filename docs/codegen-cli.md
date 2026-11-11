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

Event Modeling import remains available through the existing
[`emhcl_import` example](event-modeling-hcl.md#from-a-model-to-a-skiljtoml).
