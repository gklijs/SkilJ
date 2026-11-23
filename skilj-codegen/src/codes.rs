//! The one table of every code a `skilj-codegen` diagnostic carries
//! (docs/architecture.md §258): its category, and the one-line hint and
//! the agent guide section (`<skill>` or `<skill>#<anchor>`, a skill under
//! `skilj/agent-guides/`) where a guide section explains the code. A hint
//! and a guide come together or not at all.
//!
//! `skilj/src/error_codes.md` lists the same rows, and
//! `scripts/agent-guides/check.mjs` holds the two, and every code a problem
//! is built with, to each other.

/// One code: what a `ValidationProblem` or the binary's diagnostic carries.
pub(crate) struct Code {
    /// The stable, specific code.
    pub(crate) code: &'static str,
    /// The coarse category the binary also reports: `parse`, `semantic`,
    /// `io`, `generation`, `drift` or `usage`.
    pub(crate) category: &'static str,
    /// What to change, in one line.
    pub(crate) hint: Option<&'static str>,
    /// The guide section that explains the code.
    pub(crate) guide: Option<&'static str>,
}

const fn code(code: &'static str, category: &'static str) -> Code {
    Code {
        code,
        category,
        hint: None,
        guide: None,
    }
}

const fn guided(
    code: &'static str,
    category: &'static str,
    hint: &'static str,
    guide: &'static str,
) -> Code {
    Code {
        code,
        category,
        hint: Some(hint),
        guide: Some(guide),
    }
}

/// Every code, one per line.
#[rustfmt::skip]
pub(crate) const CODES: &[Code] = &[
    // One per distinct rule in spec.rs, and import's.
    code("invalid_bounded_context_name", "semantic"),
    code("duplicate_generated_item", "semantic"),
    code("invalid_blind_tag_name", "semantic"),
    guided("unknown_normalizer", "semantic", "set normalize to \"email\", \"compact_uppercase\" or the path of a fn(&str) -> String of your own, such as \"crate::path::to_fn\"", "skilj-uniqueness#in-a-skiljtoml"),
    code("blind_tag_keep_with_subject", "semantic"),
    guided("blind_tag_subject_missing", "semantic", "give the erasure = \"with_subject\" blind tag a subject: the subject_key whose forgetSubject removes the hashes", "skilj-uniqueness#in-a-skiljtoml"),
    guided("duplicate_blind_tag_key", "semantic", "declare one [[blind_tag]] per tag key", "skilj-uniqueness#the-pattern"),
    guided("blind_subject_tag_is_blind", "semantic", "make the subject_tag a plain tag key, one without a [[blind_tag]] of its own", "skilj-uniqueness#choosing-the-erasure"),
    guided("blind_subject_tag_missing", "semantic", "every type with the blind tag must also tag its subject_tag plainly", "skilj-uniqueness#choosing-the-erasure"),
    guided("blob_field_not_blob", "semantic", "a blobs entry must name a field of this type declared with type = \"blob\"", "skilj-large-payloads#declaring"),
    code("blob_subject_field_not_declared", "semantic"),
    guided("sensitive_blob_field", "semantic", "never declare a blob field sensitive; give it a subject in blobs instead", "skilj-large-payloads#declaring"),
    code("tag_on_blob_field", "semantic"),
    code("invalid_type_name", "semantic"),
    code("invalid_field_name", "semantic"),
    code("duplicate_field", "semantic"),
    guided("tag_field_not_declared", "semantic", "map the tag to a field the type declares in fields (check the spelling), or declare the field", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    guided("sensitive_field_not_declared", "semantic", "name fields the type declares in fields, for both field and subject_field", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    guided("tag_on_sensitive_field", "semantic", "make the tag key a [[blind_tag]] so the value is stored as a keyed hash, not in the clear", "skilj-uniqueness#first-is-the-value-personal-data"),
    guided("reserved_event_type_name", "semantic", "EventRetracted, EventDoubted and DoubtLifted are skilj's built-in event types: rename your type, or remove its declaration and enable retractions/doubts so skilj registers the built-in", "skilj#when-an-event-was-wrong"),
    guided("retractable_without_retractions", "semantic", "set retractions = true at the top of the .skilj.toml", "skilj-uniqueness#in-a-skiljtoml"),
    guided("retractable_query_latest", "semantic", "remove latest from the retractable command's query items: they read every match", "skilj-uniqueness#in-a-skiljtoml"),
    guided("reads_nothing_with_query", "semantic", "keep either reads_nothing = true or the query items, not both", "skilj#decision-tree"),
    guided("empty_query_item", "semantic", "give the query item event_types or tags, or set reads_nothing = true for a command that needs no history", "skilj#decision-tree"),
    guided("query_latest_zero", "semantic", "set latest to 1 or more, or leave it out to read every match", "skilj#decision-tree"),
    guided("query_event_type_not_declared", "semantic", "name an event type this file declares, or a built-in one that retractions or doubts registers", "skilj#decision-tree"),
    guided("query_tag_not_declared", "semantic", "name a key of the command type's own tags", "skilj#decision-tree"),
    code("no_events_for_bounded_context", "semantic"),
    // skilj-codegen's own field-type parsing.
    code("unknown_field_type", "parse"),
    // Whole-run codes: the category is the code.
    code("parse", "parse"),
    code("io", "io"),
    code("generation", "generation"),
    code("drift", "drift"),
    code("usage", "usage"),
];

/// The table's row for `code`.
pub(crate) fn lookup(code: &str) -> Option<&'static Code> {
    CODES.iter().find(|entry| entry.code == code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_and_guide_come_together() {
        for entry in CODES {
            assert_eq!(
                entry.hint.is_some(),
                entry.guide.is_some(),
                "{}: a hint needs a guide and a guide a hint",
                entry.code
            );
            if let Some(hint) = entry.hint {
                assert!(!hint.contains('\n'), "{}: one line", entry.code);
            }
            if let Some(guide) = entry.guide {
                assert!(!guide.contains('/'), "{}: <skill>#<anchor>", entry.code);
            }
        }
    }

    #[test]
    fn codes_are_unique_snake_case_and_categorised() {
        let mut seen = std::collections::BTreeSet::new();
        for entry in CODES {
            assert!(seen.insert(entry.code), "{} twice", entry.code);
            assert!(
                entry
                    .code
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '_'),
                "{}",
                entry.code
            );
            assert!(
                ["parse", "semantic", "io", "generation", "drift", "usage"]
                    .contains(&entry.category),
                "{}",
                entry.code
            );
        }
    }

    /// The literal first argument of every `marker(` call in `source`.
    fn literal_codes<'a>(source: &'a str, marker: &str) -> Vec<&'a str> {
        source
            .match_indices(marker)
            .filter_map(|(at, _)| {
                let rest = source[at + marker.len()..].trim_start();
                let rest = rest.strip_prefix('"')?;
                Some(&rest[..rest.find('"')?])
            })
            .collect()
    }

    /// Every code a problem is built with is a row of the table.
    #[test]
    fn every_code_a_problem_is_built_with_is_in_the_table() {
        let mut found = Vec::new();
        for source in [
            include_str!("spec.rs"),
            include_str!("lib.rs"),
            include_str!("import/mod.rs"),
            include_str!("main.rs"),
        ] {
            for marker in ["problems.push(", "problem(", "Diagnostic::new("] {
                found.extend(literal_codes(source, marker));
            }
        }
        assert!(found.len() > 30, "{found:?}");
        for code in found {
            assert!(lookup(code).is_some(), "{code} is not in CODES");
        }
    }
}
