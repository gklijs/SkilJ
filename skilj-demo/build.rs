//! issue #5's narrower cut ([docs/architecture.md §17](../docs/architecture.md#event-command-codegen-real)) -
//! `src/banking.skilj.toml`'s declarative event/command shape becomes
//! real Rust here, at `$OUT_DIR/banking_generated.rs`, which
//! `src/banking.rs` itself `include!()`s. Runs on every build (`cargo
//! build`'s own `build.rs` contract) - the issue's own preferred
//! default over a separate "did you remember to re-run the generator"
//! CLI step, so the generated code can never drift out of sync with the
//! `.skilj.toml` file it came from. `tests/generated_code.rs` keeps a
//! checked-in copy of the output, so changes to it show up in review
//! (docs/architecture.md §191).
//!
//! `courses.rs` has no `.skilj.toml` counterpart and isn't touched by
//! this - it stays fully hand-written, deliberately (see `banking.rs`'s
//! own doc comment for why).

//!
//! `tests/customers.skilj.toml` is generated here too, for
//! `tests/customers.rs` alone: its blind tag (docs/architecture.md §205)
//! needs a `blind_index_key`, which the demo's own bounded contexts and
//! server don't configure, so it stays out of the library.

use std::path::PathBuf;

fn main() {
    generate("src/banking.skilj.toml", "banking_generated.rs");
    generate("tests/customers.skilj.toml", "customers_generated.rs");
}

fn generate(toml_path: &str, output: &str) {
    println!("cargo:rerun-if-changed={toml_path}");

    let toml_source = std::fs::read_to_string(toml_path)
        .unwrap_or_else(|e| panic!("failed to read {toml_path}: {e}"));
    let generated = skilj_codegen::generate(&toml_source)
        .unwrap_or_else(|e| panic!("skilj-codegen failed on {toml_path}: {e}"));

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("cargo always sets OUT_DIR"));
    std::fs::write(out_dir.join(output), generated)
        .unwrap_or_else(|e| panic!("failed to write {output} to OUT_DIR: {e}"));
}
