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

#[test]
fn import_writes_the_skilj_toml_and_the_notes_apart() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    // The subcommand gives what the emhcl_import example gave, which the
    // checked-in import recorded.
    let output = run(&[
        "import",
        "--from",
        "emhcl",
        path(&fixtures.join("banking.emhcl.json")),
        "banking",
    ]);
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        output.stdout,
        std::fs::read(fixtures.join("banking.imported.skilj.toml")).unwrap()
    );

    // Every source's banking fixture, to a file `check` accepts.
    let dir = tempfile::tempdir().unwrap();
    for (from, input) in [
        ("em-json", "banking.em.json"),
        ("em-json", "banking.prooph-board.em.json"),
        ("evml", "banking.evml"),
    ] {
        let model = dir.path().join(format!("{input}.skilj.toml"));
        let output = run(&[
            "import",
            "--from",
            from,
            path(&fixtures.join(input)),
            "banking",
            "--output",
            path(&model),
        ]);
        assert!(output.status.success(), "{input}");
        assert!(
            output.stdout.is_empty() && output.stderr.is_empty(),
            "{input}"
        );
        let checked = run(&["check", path(&model)]);
        assert!(checked.status.success(), "{input}");
    }

    // Notes go to stderr, as lines or as one JSON object.
    let model = dir.path().join("orders.evml");
    std::fs::write(
        &model,
        "eventmodeling\ntf 01 cmd PlaceOrder { id: 'o-1' }\ntf 02 evt orders.OrderPlaced { id: 'o-1' }\n",
    )
    .unwrap();
    let output = run(&["import", "--from", "evml", path(&model), "orders"]);
    assert!(output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.starts_with("note: orders.OrderPlaced (frame 02): no field has `tag`"),
        "{stderr}"
    );
    let output = run(&[
        "import",
        "--from",
        "evml",
        path(&model),
        "orders",
        "--diagnostic-format",
        "json",
    ]);
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["notes"].as_array().unwrap().len(), 2);
}

#[test]
fn import_failures_have_the_statuses_of_the_other_commands() {
    let dir = tempfile::tempdir().unwrap();
    let model = dir.path().join("broken.evml");
    std::fs::write(&model, "eventmodeling\n\ntf 01 widget X\n").unwrap();
    let args = |from: &'static str, input: &Path, context: &'static str| {
        run(&[
            "import",
            "--from",
            from,
            path(input),
            context,
            "--diagnostic-format",
            "json",
        ])
    };
    let json = diagnostics(&args("evml", &model, "orders"), 1);
    assert_eq!(json["diagnostics"][0]["code"], "parse");
    assert_eq!(json["diagnostics"][0]["line"], 3);

    let json = diagnostics(&args("em-json", &model, "orders"), 1);
    assert_eq!(json["diagnostics"][0]["code"], "parse");

    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/banking.evml");
    let json = diagnostics(&args("evml", &fixture, "shipping"), 1);
    assert_eq!(json["diagnostics"][0]["code"], "semantic");

    let json = diagnostics(&args("evml", &dir.path().join("missing.evml"), "orders"), 3);
    assert_eq!(json["diagnostics"][0]["code"], "io");

    let json = diagnostics(&args("qlerify", &fixture, "banking"), 2);
    assert_eq!(json["diagnostics"][0]["code"], "usage");
}
