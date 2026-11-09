//! Imports a bounded context from an Event Modeling HCL model into a
//! `.skilj.toml` (docs/event-modeling-hcl.md):
//!
//! ```sh
//! emhcl export model/ -o model.json
//! cargo run -p skilj-codegen --example emhcl_import -- model.json banking > src/banking.skilj.toml
//! ```
//!
//! The `.skilj.toml` goes to standard output, the notes to standard error.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [json_path, bounded_context] = args.as_slice() else {
        eprintln!("usage: emhcl_import <emhcl export JSON> <bounded context>");
        return ExitCode::from(2);
    };
    let json = match std::fs::read_to_string(json_path) {
        Ok(json) => json,
        Err(e) => {
            eprintln!("can't read {json_path}: {e}");
            return ExitCode::FAILURE;
        }
    };
    match skilj_codegen::emhcl::import(&json, bounded_context) {
        Ok(import) => {
            for note in &import.notes {
                eprintln!("note: {note}");
            }
            print!("{}", import.skilj_toml);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
