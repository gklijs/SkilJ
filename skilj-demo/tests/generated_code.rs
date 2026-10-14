//! What `skilj-codegen` generates from `src/banking.skilj.toml` and
//! `tests/customers.skilj.toml`, checked in so that a change to either
//! shows up in review (docs/architecture.md §191, Codeberg issue #54).
//! `build.rs` still generates the code that is compiled; this compares
//! exactly those files, from `OUT_DIR`, against the checked-in copies in
//! `fixtures/`. A difference fails here until it is re-recorded on
//! purpose: `SKILJ_RECORD_GENERATED=1 cargo test -p skilj-demo --test generated_code`.

/// Re-records the checked-in copies instead of comparing against them.
const RECORD_ENV: &str = "SKILJ_RECORD_GENERATED";

fn matches_the_checked_in_copy(generated: &str, recorded_path: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(recorded_path);
    if std::env::var_os(RECORD_ENV).is_some() {
        std::fs::write(&path, generated).unwrap();
        return;
    }
    let recorded = std::fs::read_to_string(&path).unwrap_or_default();
    let differing: Vec<_> = generated
        .lines()
        .zip(recorded.lines())
        .enumerate()
        .filter(|(_, (now, then))| now != then)
        .map(|(i, (now, then))| format!("  line {}:\n  recorded: {then}\n  now:      {now}", i + 1))
        .collect();
    assert!(
        differing.is_empty() && generated.lines().count() == recorded.lines().count(),
        "the generated code no longer matches {recorded_path} - if that's intended, \
         re-record it with {RECORD_ENV}=1 and review the diff:\n{}",
        differing.join("\n")
    );
}

/// The code `build.rs` generated and `src/banking.rs` compiles.
#[test]
fn the_generated_banking_code_matches_the_checked_in_copy() {
    matches_the_checked_in_copy(
        include_str!(concat!(env!("OUT_DIR"), "/banking_generated.rs")),
        "tests/fixtures/banking_generated.rs",
    );
}

/// The code `build.rs` generated and `tests/customers.rs` compiles.
#[test]
fn the_generated_customers_code_matches_the_checked_in_copy() {
    matches_the_checked_in_copy(
        include_str!(concat!(env!("OUT_DIR"), "/customers_generated.rs")),
        "tests/fixtures/customers_generated.rs",
    );
}
