//! Codeberg issue #5's narrower cut ([docs/architecture.md §17](../docs/architecture.md#event-command-codegen-real)) -
//! `src/banking.skilj.toml`'s declarative event/command shape becomes
//! real Rust here, at `$OUT_DIR/banking_generated.rs`, which
//! `src/banking.rs` itself `include!()`s. Runs on every build (`cargo
//! build`'s own `build.rs` contract) - the issue's own preferred
//! default over a separate "did you remember to re-run the generator"
//! CLI step, so the generated code can never drift out of sync with the
//! `.skilj.toml` file it came from.
//!
//! `courses.rs` has no `.skilj.toml` counterpart and isn't touched by
//! this - it stays fully hand-written, deliberately (see `banking.rs`'s
//! own doc comment for why).

use std::path::PathBuf;

fn main() {
    let toml_path = "src/banking.skilj.toml";
    println!("cargo:rerun-if-changed={toml_path}");

    let toml_source = std::fs::read_to_string(toml_path)
        .unwrap_or_else(|e| panic!("failed to read {toml_path}: {e}"));
    let generated = skilj_codegen::generate(&toml_source)
        .unwrap_or_else(|e| panic!("skilj-codegen failed on {toml_path}: {e}"));

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("cargo always sets OUT_DIR"));
    std::fs::write(out_dir.join("banking_generated.rs"), generated)
        .expect("failed to write banking_generated.rs to OUT_DIR");
}
