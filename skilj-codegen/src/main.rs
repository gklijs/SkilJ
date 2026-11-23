use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::ExitCode,
};

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

/// One diagnostic of the JSON document `{"version":2,"diagnostics":[...]}`
/// (docs/codegen-cli.md).
#[derive(Serialize)]
struct Diagnostic {
    /// The stable, specific code (`tag_on_sensitive_field`, `parse`, ...).
    code: &'static str,
    /// `parse`, `semantic`, `io`, `generation`, `drift` or `usage`; what
    /// `code` was in version 1.
    category: &'static str,
    path: String,
    context: Option<String>,
    message: String,
    /// One-based line and Unicode character column; absent for semantic errors.
    line: Option<usize>,
    column: Option<usize>,
    /// What to change, where a guide section explains the code.
    hint: Option<&'static str>,
    /// That guide section, `<skill>` or `<skill>#<anchor>`.
    guide: Option<&'static str>,
}

impl Diagnostic {
    /// A whole-run diagnostic (`parse`, `io`, `generation`, `drift`,
    /// `usage`), whose category is its code.
    fn new(code: &'static str, path: &Path, message: impl ToString) -> Self {
        Self {
            code,
            category: code,
            path: path.to_string_lossy().into_owned(),
            context: None,
            message: message.to_string(),
            line: None,
            column: None,
            hint: None,
            guide: None,
        }
    }

    /// Sets `line` and `column` to where `offset` is in the source.
    fn at(mut self, lines: &LineIndex<'_>, offset: usize) -> Self {
        let (line, column) = lines.position(offset);
        self.line = Some(line);
        self.column = Some(column);
        self
    }
}

/// The one conversion of a library problem: its code, category, hint and
/// guide forwarded as they are.
impl From<(&Path, &skilj_codegen::ValidationProblem)> for Diagnostic {
    fn from((path, problem): (&Path, &skilj_codegen::ValidationProblem)) -> Self {
        Self {
            code: problem.code,
            category: problem.category(),
            path: path.to_string_lossy().into_owned(),
            context: (!problem.context.is_empty()).then(|| problem.context.clone()),
            message: problem.message.clone(),
            line: None,
            column: None,
            hint: problem.hint,
            guide: problem.guide,
        }
    }
}

/// Where each line of a source starts, built once per source, so every
/// diagnostic's position is a binary search rather than a scan.
struct LineIndex<'a> {
    source: &'a str,
    starts: Vec<usize>,
}

impl<'a> LineIndex<'a> {
    fn new(source: &'a str) -> Self {
        let starts = std::iter::once(0)
            .chain(source.match_indices('\n').map(|(at, _)| at + 1))
            .collect();
        Self { source, starts }
    }

    /// The one-based line and Unicode character column of a byte offset,
    /// the convention of toml's own error positions.
    fn position(&self, offset: usize) -> (usize, usize) {
        let mut offset = offset.min(self.source.len());
        while !self.source.is_char_boundary(offset) {
            offset -= 1;
        }
        let line = self.starts.partition_point(|&start| start <= offset);
        let start = self.starts[line - 1];
        (line, self.source[start..offset].chars().count() + 1)
    }
}

type Failure = (u8, Vec<Diagnostic>);

fn io_error(path: &Path, error: std::io::Error) -> Failure {
    (3, vec![Diagnostic::new("io", path, error)])
}

/// Writes beside the destination and renames only after the complete
/// artifact has been written. Failure leaves existing output intact.
fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), Failure> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
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
    input: &Path,
    bounded_context: &str,
    output: Option<&Path>,
) -> Result<(), Failure> {
    let source = std::fs::read_to_string(input).map_err(|e| io_error(input, e))?;
    let import = skilj_codegen::import::import(from.source(), &source, bounded_context).map_err(
        |error| match &error {
            skilj_codegen::Error::Json(_) => (1, vec![Diagnostic::new("parse", input, &error)]),
            skilj_codegen::Error::Evml { line, .. } => {
                let mut diagnostic = Diagnostic::new("parse", input, &error);
                diagnostic.line = Some(*line);
                (1, vec![diagnostic])
            }
            skilj_codegen::Error::Invalid(problems) => (
                1,
                problems
                    .iter()
                    .map(|problem| Diagnostic::from((input, problem)))
                    .collect(),
            ),
            _ => (3, vec![Diagnostic::new("generation", input, &error)]),
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

/// A library error that ends the run: a TOML error is `parse` with its
/// position, exit 1; anything else is a bug in skilj-codegen (`generation`,
/// exit 3).
fn generation_failure(input: &Path, lines: &LineIndex<'_>, error: skilj_codegen::Error) -> Failure {
    match &error {
        skilj_codegen::Error::Toml(toml) => {
            let diagnostic = Diagnostic::new("parse", input, &error);
            let diagnostic = match toml.span() {
                Some(span) => diagnostic.at(lines, span.start),
                None => diagnostic,
            };
            (1, vec![diagnostic])
        }
        skilj_codegen::Error::Invalid(problems) => (1, positioned(input, lines, problems)),
        _ => (3, vec![Diagnostic::new("generation", input, &error)]),
    }
}

/// The problems of `input`'s model as diagnostics, each with its position
/// where it has a span.
fn positioned(
    input: &Path,
    lines: &LineIndex<'_>,
    problems: &[skilj_codegen::ValidationProblem],
) -> Vec<Diagnostic> {
    problems
        .iter()
        .map(|problem| {
            let diagnostic = Diagnostic::from((input, problem));
            match &problem.span {
                Some(span) => diagnostic.at(lines, span.start),
                None => diagnostic,
            }
        })
        .collect()
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
    let lines = LineIndex::new(&source);
    let problems = skilj_codegen::validate(&source)
        .map_err(|error| generation_failure(input, &lines, error))?;
    if !problems.is_empty() {
        return Err((1, positioned(input, &lines, &problems)));
    }
    let generated = skilj_codegen::generate(&source)
        .map_err(|error| generation_failure(input, &lines, error))?;
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

/// Prints the diagnostics to stderr: one JSON document, or per diagnostic
/// `<path>: <code>: <message>` (`<code>: <message>` without a path), then its
/// hint and guide lines where it has them.
fn report(format: Format, diagnostics: &[Diagnostic]) {
    match format {
        Format::Json => eprintln!(
            "{}",
            serde_json::json!({"version": 2, "diagnostics": diagnostics})
        ),
        Format::Text => {
            for diagnostic in diagnostics {
                if diagnostic.path.is_empty() {
                    eprintln!("{}: {}", diagnostic.code, diagnostic.message);
                } else {
                    eprintln!(
                        "{}: {}: {}",
                        diagnostic.path, diagnostic.code, diagnostic.message
                    );
                }
                if let Some(hint) = diagnostic.hint {
                    eprintln!("  hint: {hint}");
                }
                if let Some(guide) = diagnostic.guide {
                    eprintln!("  guide: {guide}");
                }
            }
        }
    }
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
            let format = if json_requested {
                Format::Json
            } else {
                Format::Text
            };
            // `render()` without color: the same text a terminal would show.
            let message = error.render().to_string();
            report(
                format,
                &[Diagnostic::new("usage", Path::new(""), message.trim_end())],
            );
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
            report(cli.diagnostic_format, &diagnostics);
            ExitCode::from(status)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// generated code that doesn't parse is skilj-codegen's bug: `generation`,
    /// exit 3, whatever the input.
    #[test]
    fn generated_code_that_does_not_parse_is_a_generation_failure() {
        let error = skilj_codegen::Error::Generated(syn::Error::new(
            proc_macro2::Span::call_site(),
            "expected an item",
        ));
        let (status, diagnostics) =
            generation_failure(Path::new("model.skilj.toml"), &LineIndex::new(""), error);
        assert_eq!(status, 3);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code, "generation");
        assert_eq!(diagnostics[0].category, "generation");
        assert!(diagnostics[0].hint.is_none() && diagnostics[0].guide.is_none());
    }

    #[test]
    fn positions_are_one_based_lines_and_character_columns() {
        let source = "a = 1\nnäme = 'x'\n\nlast";
        let lines = LineIndex::new(source);
        assert_eq!(lines.position(0), (1, 1));
        assert_eq!(lines.position(4), (1, 5));
        assert_eq!(lines.position(6), (2, 1));
        assert_eq!(lines.position(source.find('=').unwrap()), (1, 3));
        // After the two-byte `ä`: the column counts characters, not bytes.
        assert_eq!(source.rfind('=').unwrap(), 12);
        assert_eq!(lines.position(source.rfind('=').unwrap()), (2, 6));
        assert_eq!(lines.position(source.len()), (4, 5));
    }
}
