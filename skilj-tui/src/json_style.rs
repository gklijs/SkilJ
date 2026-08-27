//! A small JSON syntax highlighter (Codeberg issue #7's "even basic
//! key/value colour differentiation would help scanning a large
//! payload") - walks a `serde_json::Value` and produces styled ratatui
//! `Line`/`Span`s, replacing `ui.rs`'s previous plain
//! `serde_json::to_string_pretty`/`.to_string()` calls. Pure and
//! unit-testable (token → style mapping, not terminal rendering), the
//! same way `form.rs`'s schema classifier already is.
//!
//! Leaf tokens (strings/numbers/bools/null) are rendered via
//! `serde_json::to_string` on that one value - reusing `serde_json`'s
//! own, already-correct escaping rather than re-deriving it (a
//! hand-rolled string-escaper is exactly the kind of thing that quietly
//! mis-renders a payload containing a `"` or `\n`).

use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use serde_json::Value;

const INDENT: &str = "  ";

fn key_style() -> Style {
    Style::default().fg(Color::Cyan)
}
fn string_style() -> Style {
    Style::default().fg(Color::Green)
}
fn number_style() -> Style {
    Style::default().fg(Color::Yellow)
}
fn bool_style() -> Style {
    Style::default().fg(Color::Magenta)
}
fn null_style() -> Style {
    Style::default().fg(Color::DarkGray)
}
fn punctuation_style() -> Style {
    Style::default().fg(Color::DarkGray)
}

fn leaf_span(value: &Value) -> Span<'static> {
    let text = serde_json::to_string(value).unwrap_or_default();
    let style = match value {
        Value::String(_) => string_style(),
        Value::Number(_) => number_style(),
        Value::Bool(_) => bool_style(),
        Value::Null => null_style(),
        Value::Object(_) | Value::Array(_) => Style::default(),
    };
    Span::styled(text, style)
}

fn is_leaf(value: &Value) -> bool {
    !matches!(value, Value::Object(_) | Value::Array(_))
}

/// Multi-line, indented rendering - `Commands`/`Projections`' own Result
/// panels (replacing `ui.rs`'s previous `pretty()`).
pub fn pretty(value: &Value) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    let mut current = Vec::new();
    write_pretty(value, 0, &mut lines, &mut current);
    if !current.is_empty() {
        lines.push(Line::from(std::mem::take(&mut current)));
    }
    lines
}

fn write_pretty(value: &Value, depth: usize, lines: &mut Vec<Line<'static>>, current: &mut Vec<Span<'static>>) {
    match value {
        Value::Object(map) if map.is_empty() => current.push(Span::styled("{}", punctuation_style())),
        Value::Object(map) => {
            current.push(Span::styled("{", punctuation_style()));
            lines.push(Line::from(std::mem::take(current)));
            let last = map.len() - 1;
            for (i, (key, val)) in map.iter().enumerate() {
                current.push(Span::raw(INDENT.repeat(depth + 1)));
                current.push(Span::styled(format!("{key:?}"), key_style()));
                current.push(Span::styled(": ", punctuation_style()));
                if is_leaf(val) {
                    current.push(leaf_span(val));
                    if i != last {
                        current.push(Span::styled(",", punctuation_style()));
                    }
                    lines.push(Line::from(std::mem::take(current)));
                } else {
                    write_pretty(val, depth + 1, lines, current);
                    if i != last {
                        current.push(Span::styled(",", punctuation_style()));
                    }
                    lines.push(Line::from(std::mem::take(current)));
                }
            }
            current.push(Span::raw(INDENT.repeat(depth)));
            current.push(Span::styled("}", punctuation_style()));
        }
        Value::Array(items) if items.is_empty() => current.push(Span::styled("[]", punctuation_style())),
        Value::Array(items) => {
            current.push(Span::styled("[", punctuation_style()));
            lines.push(Line::from(std::mem::take(current)));
            let last = items.len() - 1;
            for (i, val) in items.iter().enumerate() {
                current.push(Span::raw(INDENT.repeat(depth + 1)));
                if is_leaf(val) {
                    current.push(leaf_span(val));
                    if i != last {
                        current.push(Span::styled(",", punctuation_style()));
                    }
                    lines.push(Line::from(std::mem::take(current)));
                } else {
                    write_pretty(val, depth + 1, lines, current);
                    if i != last {
                        current.push(Span::styled(",", punctuation_style()));
                    }
                    lines.push(Line::from(std::mem::take(current)));
                }
            }
            current.push(Span::raw(INDENT.repeat(depth)));
            current.push(Span::styled("]", punctuation_style()));
        }
        leaf => current.push(leaf_span(leaf)),
    }
}

/// Single-line, compact rendering - `Live Events`/`Query Events`' own
/// list rows (replacing `ui.rs`'s previous `pretty_compact()`).
pub fn compact(value: &Value) -> Line<'static> {
    let mut spans = Vec::new();
    write_compact(value, &mut spans);
    Line::from(spans)
}

fn write_compact(value: &Value, spans: &mut Vec<Span<'static>>) {
    match value {
        Value::Object(map) => {
            spans.push(Span::styled("{", punctuation_style()));
            let last = map.len().wrapping_sub(1);
            for (i, (key, val)) in map.iter().enumerate() {
                spans.push(Span::styled(format!("{key:?}"), key_style()));
                spans.push(Span::styled(":", punctuation_style()));
                write_compact(val, spans);
                if i != last {
                    spans.push(Span::styled(",", punctuation_style()));
                }
            }
            spans.push(Span::styled("}", punctuation_style()));
        }
        Value::Array(items) => {
            spans.push(Span::styled("[", punctuation_style()));
            let last = items.len().wrapping_sub(1);
            for (i, val) in items.iter().enumerate() {
                write_compact(val, spans);
                if i != last {
                    spans.push(Span::styled(",", punctuation_style()));
                }
            }
            spans.push(Span::styled("]", punctuation_style()));
        }
        leaf => spans.push(leaf_span(leaf)),
    }
}

/// The plain-text equivalent of [`compact`] - `Live Events`' own
/// substring filter matches against this (Codeberg issue #7), never
/// against styled spans, which have no single obvious "text" to search.
pub fn compact_plain(value: &Value) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn plain_text(line: &Line<'_>) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn a_string_leaf_is_quoted_and_escaped_the_same_way_serde_json_would() {
        let lines = pretty(&json!("hello \"world\""));
        assert_eq!(lines.len(), 1);
        assert_eq!(plain_text(&lines[0]), r#""hello \"world\"""#);
        assert_eq!(lines[0].spans[0].style, string_style());
    }

    #[test]
    fn a_number_and_a_bool_get_distinct_styles() {
        let number_lines = pretty(&json!(42));
        assert_eq!(number_lines[0].spans[0].style, number_style());
        let bool_lines = pretty(&json!(true));
        assert_eq!(bool_lines[0].spans[0].style, bool_style());
    }

    #[test]
    fn an_object_renders_one_key_per_line_with_the_key_styled_distinctly() {
        let lines = pretty(&json!({"a": 1}));
        // "{", "  \"a\": 1", "}"
        assert_eq!(lines.len(), 3);
        assert_eq!(plain_text(&lines[0]), "{");
        assert_eq!(plain_text(&lines[1]), "  \"a\": 1");
        assert_eq!(plain_text(&lines[2]), "}");
        let key_span = lines[1].spans.iter().find(|s| s.content.as_ref() == "\"a\"").unwrap();
        assert_eq!(key_span.style, key_style());
    }

    #[test]
    fn an_empty_object_and_array_render_on_one_line() {
        assert_eq!(pretty(&json!({})).len(), 1);
        assert_eq!(pretty(&json!([])).len(), 1);
    }

    #[test]
    fn nested_objects_indent_one_level_deeper() {
        let lines = pretty(&json!({"a": {"b": 1}}));
        // {, "a": {, "b": 1 (indented two levels), }, }
        let inner = lines.iter().find(|l| plain_text(l).contains("\"b\"")).unwrap();
        assert!(plain_text(inner).starts_with("    "), "nested field should be double-indented");
    }

    #[test]
    fn compact_renders_everything_on_one_line_with_no_spaces() {
        let line = compact(&json!({"a": 1, "b": [true, null]}));
        assert_eq!(plain_text(&line), r#"{"a":1,"b":[true,null]}"#);
    }

    #[test]
    fn compact_plain_matches_a_plain_to_string_for_filtering() {
        let value = json!({"account_id": "a1", "amount": 42});
        assert_eq!(compact_plain(&value), value.to_string());
    }
}
