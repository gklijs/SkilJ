//! `build.rs` codegen for skilj's own declarative bounded-context
//! format, Codeberg issue #5's "narrower cut" (see `docs/architecture.md`
//! [§17](../../docs/architecture.md#event-command-codegen-real) for the full design, and [§16](../../docs/architecture.md#declarative-bounded-context-codegen-prototype) for the prototype that scoped it
//! down to this). A `.skilj.toml` file describes one bounded context's
//! event/command type *shapes* only, fields, DCB tags and
//! `rest_trigger_allowed`, and this crate turns that into real Rust:
//! one payload struct and one `EventType`/`CommandType` impl per
//! declared type, plus the shared per-bounded-context event enum and
//! its `BoundedContextEvent` impl.
//!
//! **Deliberately narrow, not a gap to widen casually.**
//! `sensitive_fields`, the event creation-origin flags
//! (`external_creation_allowed`/`direct_creation_allowed`/
//! `event_read_allowed`), scheduling, `#[requires_role]`, and every
//! `Projection` concept are real, legitimate parts of the plugin API
//! this format doesn't cover - the [§16](../../docs/architecture.md#declarative-bounded-context-codegen-prototype) prototype's own recommendation
//! was to prove the mechanism on exactly what a real conversion needed
//! (`skilj-demo/src/banking.rs`, which uses none of those), not to
//! guess ahead of a second real use case.
//!
//! **A spec that can't produce working code is refused up front.**
//! [`generate`] checks names (the bounded context's against skilj's own
//! rule; type and field names as Rust identifiers - a keyword field such
//! as `type` is fine, emitted as `r#type`), duplicate fields, tags naming
//! undeclared fields, and any two declarations generating the same Rust
//! item, and returns every problem at once as [`Error::Invalid`] - rather
//! than panicking in `build.rs`, failing inside generated code, or
//! failing registration at startup.
//!
//! **`decide()`/`project()` stay hand-written Rust, always.** A
//! generated `CommandType::decide()` is one line, delegating to a
//! plain free function (`decide_<snake_case(NAME)>`) the including
//! module is expected to already define - see `emit::emit_command_type`'s
//! own doc comment. This crate never sees, needs, or could sensibly
//! generate real domain logic.
//!
//! **How a consumer uses this**: from its own `build.rs`, call
//! [`generate`] on a `.skilj.toml` file's contents, write the result to
//! `$OUT_DIR`, and `include!()` it from the hand-written module that
//! also defines the `decide_*` functions. See
//! `skilj-demo/build.rs`/`skilj-demo/src/banking.rs` for the real,
//! working example.
//!
//! **Reviewing what it generates.** Code written to `$OUT_DIR` never
//! shows up in a diff. `skilj-demo/tests/generated_code.rs` compares the
//! `$OUT_DIR` file against a checked-in copy, so a change to the
//! `.skilj.toml` or to this crate shows up as a diff of that copy
//! (docs/architecture.md §191). A consumer can do the same in a few
//! lines.

mod emit;
mod spec;

pub use spec::{BoundedContextSpec, CommandTypeSpec, EventTypeSpec, FieldSpec, FieldType};

#[derive(Debug)]
pub enum Error {
    /// The `.skilj.toml` file itself doesn't parse, or doesn't match
    /// the expected shape - a real user-facing error, surfaced through
    /// `build.rs` as a build failure with `toml`'s own message.
    Toml(toml::de::Error),
    /// The `.skilj.toml` parses but can't produce working code - an
    /// invalid name, a duplicate, a tag naming an undeclared field, two
    /// types generating the same Rust item. Every problem found, not just
    /// the first.
    Invalid(Vec<String>),
    /// The `TokenStream` this crate emitted doesn't parse as a valid
    /// `syn::File` - a bug in this crate's own `emit` module, never a
    /// user error; nothing about a well-formed `.skilj.toml` file
    /// should be able to trigger this.
    Generated(syn::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Toml(e) => write!(f, "invalid .skilj.toml: {e}"),
            Error::Invalid(problems) => {
                write!(f, "invalid .skilj.toml:")?;
                for problem in problems {
                    write!(f, "\n  - {problem}")?;
                }
                Ok(())
            }
            Error::Generated(e) => {
                write!(f, "skilj-codegen generated code that failed to parse - this is a bug in skilj-codegen itself, not in your .skilj.toml: {e}")
            }
        }
    }
}

impl std::error::Error for Error {}

/// Parses `toml_source` (a `.skilj.toml` file's own contents) and
/// returns real, `prettyplease`-formatted Rust source - genuinely
/// readable when a consumer's own `build.rs` writes it to `$OUT_DIR`
/// for debugging, not a minified one-liner. See this crate's own root
/// doc comment for what the output covers.
pub fn generate(toml_source: &str) -> Result<String, Error> {
    let spec: BoundedContextSpec = toml::from_str(toml_source).map_err(Error::Toml)?;
    let problems = spec::validate(&spec);
    if !problems.is_empty() {
        return Err(Error::Invalid(problems));
    }
    let tokens = emit::emit(&spec);
    let file: syn::File = syn::parse2(tokens).map_err(Error::Generated)?;
    Ok(prettyplease::unparse(&file))
}
