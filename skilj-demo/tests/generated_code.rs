//! What `skilj-codegen` generates from `src/banking.skilj.toml`, checked
//! in so that a change to either shows up in review (docs/architecture.md
//! §191, Codeberg issue #54). `build.rs` still generates the code that is
//! compiled; this compares exactly that file, from `OUT_DIR`, against the
//! checked-in `fixtures/banking_generated.rs`. A difference fails here
//! until it is re-recorded on purpose:
//! `SKILJ_RECORD_GENERATED=1 cargo test -p skilj-demo --test generated_code`.

/// The code `build.rs` generated and `src/banking.rs` compiles.
const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/banking_generated.rs"));

const RECORDED_PATH: &str = "tests/fixtures/banking_generated.rs";

/// Re-records the checked-in copy instead of comparing against it.
const RECORD_ENV: &str = "SKILJ_RECORD_GENERATED";

#[test]
fn the_generated_banking_code_matches_the_checked_in_copy() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(RECORDED_PATH);
    if std::env::var_os(RECORD_ENV).is_some() {
        std::fs::write(&path, GENERATED).unwrap();
        return;
    }
    let recorded = std::fs::read_to_string(&path).unwrap_or_default();
    let differing: Vec<_> = GENERATED
        .lines()
        .zip(recorded.lines())
        .enumerate()
        .filter(|(_, (now, then))| now != then)
        .map(|(i, (now, then))| format!("  line {}:\n  recorded: {then}\n  now:      {now}", i + 1))
        .collect();
    assert!(
        differing.is_empty() && GENERATED.lines().count() == recorded.lines().count(),
        "the generated banking code no longer matches {RECORDED_PATH} - if that's intended, \
         re-record it with {RECORD_ENV}=1 and review the diff:\n{}",
        differing.join("\n")
    );
}
