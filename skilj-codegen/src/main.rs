use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::{io::Write, path::PathBuf, process::ExitCode};

#[derive(Parser)]
#[command(
    version,
    about = "Validate skilj models, preview generated Rust, and import models from Event Modeling tools"
)]
struct Cli {
    /// Diagnostic format on stderr (stdout is reserved for generated Rust).
    #[arg(long, global = true, value_enum, default_value = "text")]
    diagnostic_format: Format,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
}

#[derive(Subcommand)]
enum Command {
    /// Parse, validate, and verify that Rust can be generated.
    Check { input: PathBuf },
    /// Generate Rust to stdout, a file, or compare it against a file.
    Generate {
        input: PathBuf,
        /// Replace this file only after successful generation.
        #[arg(short, long, conflicts_with = "check")]
        output: Option<PathBuf>,
        /// Report drift against this file without modifying it.
        #[arg(long, value_name = "FILE")]
        check: Option<PathBuf>,
    },
    /// Import one bounded context from an Event Modeling tool's model into a
    /// .skilj.toml, to stdout or a file; what it left out goes to stderr.
    Import {
        /// The model's format (docs/modeling-tools.md).
        #[arg(long, value_enum)]
        from: SourceArg,
        /// The model: an `emhcl export` JSON file, an Event Modeling JSON
        /// file, or an .evml file.
        input: PathBuf,
        /// The skilj bounded context to import, lowercase snake_case.
        bounded_context: String,
        /// Replace this file only after a successful import.
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

/// `skilj_codegen::import::Source`, as `--from` names it.
#[derive(Clone, Copy, ValueEnum)]
enum SourceArg {
    /// `emhcl export` JSON of an Event Modeling HCL model.
    Emhcl,
    /// Event Modeling Specification JSON: Nebulit's Miro app, the prooph board route.
    EmJson,
    /// The text language of event-modeling-tools (.evml, Mermaid eventmodeling).
    Evml,
}

impl SourceArg {
    fn source(self) -> skilj_codegen::import::Source {
        use skilj_codegen::import::Source;
        match self {
            SourceArg::Emhcl => Source::Emhcl,
            SourceArg::EmJson => Source::EmJson,
            SourceArg::Evml => Source::Evml,
        }
    }
}

#[derive(Serialize)]
struct Diagnostic {
    code: &'static str,
    path: String,
    context: Option<String>,
    message: String,
    /// One-based line and Unicode character column; absent for semantic errors.
    line: Option<usize>,
    column: Option<usize>,
}

impl Diagnostic {
    fn new(code: &'static str, path: &std::path::Path, message: impl ToString) -> Self {
        Self {
            code,
            path: path.to_string_lossy().into_owned(),
            context: None,
            message: message.to_string(),
            line: None,
            column: None,
        }
    }
}

type Failure = (u8, Vec<Diagnostic>);

fn io_error(path: &std::path::Path, error: std::io::Error) -> Failure {
    (3, vec![Diagnostic::new("io", path, error)])
}

/// Writes beside the destination and renames only after the complete
/// artifact has been written. Failure leaves existing output intact.
fn write_atomically(path: &std::path::Path, bytes: &[u8]) -> Result<(), Failure> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|e| io_error(path, e))?;
    temporary.write_all(bytes).map_err(|e| io_error(path, e))?;
    temporary
        .persist(path)
        .map_err(|e| io_error(path, e.error))?;
    Ok(())
}

/// `import`: the `.skilj.toml` to stdout or `output`, the notes to stderr.
fn run_import(
    format: Format,
    from: SourceArg,
    input: &std::path::Path,
    bounded_context: &str,
    output: Option<&std::path::Path>,
) -> Result<(), Failure> {
    let source = std::fs::read_to_string(input).map_err(|e| io_error(input, e))?;
    let import = skilj_codegen::import::import(from.source(), &source, bounded_context).map_err(
        |error| {
            let mut diagnostic = Diagnostic::new("generation", input, &error);
            let status = match &error {
                skilj_codegen::Error::Json(_) => {
                    diagnostic.code = "parse";
                    1
                }
                skilj_codegen::Error::Evml { line, .. } => {
                    diagnostic.code = "parse";
                    diagnostic.line = Some(*line);
                    1
                }
                skilj_codegen::Error::Invalid(problems) => {
                    return (
                        1,
                        problems
                            .iter()
                            .map(|problem| Diagnostic::new("semantic", input, problem))
                            .collect(),
                    );
                }
                _ => 3,
            };
            (status, vec![diagnostic])
        },
    )?;
    match output {
        Some(path) => write_atomically(path, import.skilj_toml.as_bytes())?,
        None => std::io::stdout()
            .lock()
            .write_all(import.skilj_toml.as_bytes())
            .map_err(|e| io_error(input, e))?,
    }
    if !import.notes.is_empty() {
        match format {
            Format::Json => eprintln!(
                "{}",
                serde_json::json!({"version": 1, "notes": import.notes})
            ),
            Format::Text => {
                for note in &import.notes {
                    eprintln!("note: {note}");
                }
            }
        }
    }
    Ok(())
}

fn run(cli: &Cli) -> Result<(), Failure> {
    let input = match &cli.command {
        Command::Check { input } | Command::Generate { input, .. } => input,
        Command::Import {
            from,
            input,
            bounded_context,
            output,
        } => {
            return run_import(
                cli.diagnostic_format,
                *from,
                input,
                bounded_context,
                output.as_deref(),
            )
        }
    };
    let source = std::fs::read_to_string(input).map_err(|e| io_error(input, e))?;
    let generation_error = |error: skilj_codegen::Error| {
        let mut diagnostic = Diagnostic::new("generation", input, &error);
        let status = if let skilj_codegen::Error::Toml(error) = error {
            diagnostic.code = "parse";
            if let Some(span) = error.span() {
                let prefix = &source[..span.start];
                diagnostic.line = Some(prefix.bytes().filter(|&b| b == b'\n').count() + 1);
                diagnostic.column =
                    Some(prefix.rsplit('\n').next().unwrap_or("").chars().count() + 1);
            }
            1
        } else {
            3
        };
        (status, vec![diagnostic])
    };
    let problems = skilj_codegen::validate(&source).map_err(generation_error)?;
    if !problems.is_empty() {
        return Err((
            1,
            problems
                .into_iter()
                .map(|problem| {
                    let mut diagnostic = Diagnostic::new("semantic", input, problem.message);
                    diagnostic.context = Some(problem.context);
                    diagnostic
                })
                .collect(),
        ));
    }
    let generated = skilj_codegen::generate(&source).map_err(generation_error)?;
    if let Command::Generate { output, check, .. } = &cli.command {
        if let Some(path) = check {
            let recorded = std::fs::read(path).map_err(|e| io_error(path, e))?;
            if recorded != generated.as_bytes() {
                return Err((
                    4,
                    vec![Diagnostic::new(
                        "drift",
                        path,
                        "generated Rust differs from this file",
                    )],
                ));
            }
        } else if let Some(path) = output {
            write_atomically(path, generated.as_bytes())?;
        } else {
            std::io::stdout()
                .lock()
                .write_all(generated.as_bytes())
                .map_err(|e| io_error(input, e))?;
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    // Handle clap failures here too, so JSON diagnostics remain parseable for
    // invalid invocations. Help/version retain clap's usual stdout behavior.
    let args: Vec<_> = std::env::args_os().collect();
    let json_requested = args
        .windows(2)
        .any(|w| w[0] == "--diagnostic-format" && w[1] == "json")
        || args.iter().any(|a| a == "--diagnostic-format=json");
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) if error.use_stderr() => {
            if json_requested {
                eprintln!(
                    "{}",
                    serde_json::json!({"version": 1, "diagnostics": [Diagnostic::new("usage", std::path::Path::new(""), &error)]})
                );
            } else {
                let _ = error.print();
            }
            return ExitCode::from(2);
        }
        Err(error) => {
            let _ = error.print();
            return ExitCode::SUCCESS;
        }
    };
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err((status, diagnostics)) => {
            match cli.diagnostic_format {
                Format::Json => eprintln!(
                    "{}",
                    serde_json::json!({"version": 1, "diagnostics": diagnostics})
                ),
                Format::Text => {
                    for diagnostic in diagnostics {
                        eprintln!(
                            "{}: {}: {}",
                            diagnostic.path, diagnostic.code, diagnostic.message
                        );
                    }
                }
            }
            ExitCode::from(status)
        }
    }
}
