use std::{
    path::Path,
    process::{Command, Output},
};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_skilj-codegen"))
        .args(args)
        .output()
        .unwrap()
}
fn path(path: &Path) -> &str {
    path.to_str().unwrap()
}
fn fixture() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../skilj-demo/src/banking.skilj.toml")
}
fn diagnostics(output: &Output, status: i32) -> serde_json::Value {
    assert_eq!(
        output.status.code(),
        Some(status),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    let json: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(json["version"], 1);
    json
}

#[test]
fn generation_matches_library_and_reviewed_fixtures_and_is_deterministic() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../skilj-demo");
    for (model, recorded) in [
        (
            "src/banking.skilj.toml",
            "tests/fixtures/banking_generated.rs",
        ),
        (
            "tests/customers.skilj.toml",
            "tests/fixtures/customers_generated.rs",
        ),
    ] {
        let input = root.join(model);
        let first = run(&["generate", path(&input)]);
        assert!(first.status.success());
        assert!(first.stderr.is_empty());
        assert_eq!(
            first.stdout,
            skilj_codegen::generate(&std::fs::read_to_string(&input).unwrap())
                .unwrap()
                .as_bytes()
        );
        assert_eq!(first.stdout, std::fs::read(root.join(recorded)).unwrap());
        assert_eq!(first.stdout, run(&["generate", path(&input)]).stdout);
        let checked = run(&["check", path(&input)]);
        assert!(checked.status.success());
        assert!(checked.stdout.is_empty() && checked.stderr.is_empty());
    }
}

#[test]
fn file_output_and_drift_check_preserve_artifacts_and_support_spaces() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("generated code.rs");
    assert!(
        run(&["generate", path(&fixture()), "--output", path(&output)])
            .status
            .success()
    );
    let good = std::fs::read(&output).unwrap();
    assert!(
        run(&["generate", path(&fixture()), "--check", path(&output)])
            .status
            .success()
    );
    std::fs::write(&output, b"old artifact").unwrap();
    let result = run(&[
        "generate",
        path(&fixture()),
        "--check",
        path(&output),
        "--diagnostic-format=json",
    ]);
    let json = diagnostics(&result, 4);
    assert_eq!(json["diagnostics"][0]["code"], "drift");
    assert_eq!(std::fs::read(&output).unwrap(), b"old artifact");
    let input = dir.path().join("invalid model.skilj.toml");
    for source in ["bounded_context = [", "bounded_context = 'INVALID'"] {
        std::fs::write(&input, source).unwrap();
        diagnostics(
            &run(&[
                "generate",
                path(&input),
                "-o",
                path(&output),
                "--diagnostic-format=json",
            ]),
            1,
        );
        assert_eq!(std::fs::read(&output).unwrap(), b"old artifact");
    }
    std::fs::write(&input, std::fs::read(fixture()).unwrap()).unwrap();
    assert!(run(&["generate", path(&input), "-o", path(&output)])
        .status
        .success());
    assert_eq!(std::fs::read(&output).unwrap(), good);
}

#[test]
fn json_reports_all_semantic_errors_with_declaration_context_and_no_fake_spans() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("model.skilj.toml");
    std::fs::write(&input, "bounded_context = 'INVALID'\n[[event_type]]\nname = 'Changed'\nfields = [{name = 'bad-name', type = 'string'}]\n").unwrap();
    let json = diagnostics(
        &run(&["check", path(&input), "--diagnostic-format", "json"]),
        1,
    );
    let entries = json["diagnostics"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["context"], "bounded_context");
    assert_eq!(entries[1]["context"], "event_type \"Changed\"");
    for entry in entries {
        assert_eq!(entry["code"], "semantic");
        assert_eq!(entry["path"], path(&input));
        assert!(entry["line"].is_null() && entry["column"].is_null());
        assert!(entry["message"].is_string());
    }
}

#[test]
fn parse_io_and_usage_failures_have_distinct_statuses_and_json() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("model.skilj.toml");
    let json = diagnostics(
        &run(&["check", path(&input), "--diagnostic-format=json"]),
        3,
    );
    assert_eq!(json["diagnostics"][0]["code"], "io");
    std::fs::write(&input, "bounded_context = 'banking'\n@\n").unwrap();
    let json = diagnostics(
        &run(&["check", path(&input), "--diagnostic-format=json"]),
        1,
    );
    let entry = &json["diagnostics"][0];
    assert_eq!(entry["code"], "parse");
    assert_eq!(entry["line"], 2);
    // The TOML parser locates this error immediately after the invalid token.
    assert_eq!(entry["column"], 2);
    let json = diagnostics(&run(&["--diagnostic-format=json", "generate"]), 2);
    assert_eq!(json["diagnostics"][0]["code"], "usage");
    diagnostics(
        &run(&[
            "--diagnostic-format=json",
            "generate",
            path(&fixture()),
            "-o",
            "out",
            "--check",
            "out",
        ]),
        2,
    );
    let result = run(&[
        "generate",
        path(&fixture()),
        "-o",
        path(&dir.path().join("missing/out.rs")),
        "--diagnostic-format=json",
    ]);
    diagnostics(&result, 3);
}
