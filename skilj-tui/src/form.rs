//! Generates a small form from a command/event type's own JSON Schema
//! (`TypeRegistration`'s `commandTypes`/`eventTypes` queries, Codeberg
//! issue #6's "5a" - each result carries `schema` alongside `name`) -
//! `app.rs`'s Commands/Query Events tabs both build one of these once a
//! real registered type is picked, replacing v1's raw-JSON payload entry
//! (docs/architecture.md §11's own "No schema-driven command/event forms
//! in v1" note). Pure and I/O-free, unlike `projection_query.rs`'s own
//! generic-shape mechanism - that one *has* to be a live GraphQL
//! introspection walk, since a projection's wire type is dynamic
//! (§5.1); a command/event type's shape is just data already sitting on
//! the type itself, no round trip needed once the type is picked.
//!
//! **Shapes classified, verified against real `schemars` 0.8 output**
//! (a throwaway probe crate, the same verification discipline this
//! project's own prior JSON-Schema-touching passes already held
//! themselves to - never assumed):
//! - `"type": "string"` (bare, or `["string","null"]` for
//!   `Option<String>` - schemars renders optionality as a type array,
//!   never a second key) → [`Widget::Text`].
//! - `"type": "integer"`/`"number"` (same array handling for `Option<T>`)
//!   → [`Widget::Number`], parsed into a `serde_json::Number` only at
//!   [`assemble_payload`] time.
//! - `"type": "boolean"` → [`Widget::Bool`], a real toggle rather than
//!   typed text.
//! - Everything else - `"type": "object"`/`"array"`, or a bare `"$ref"`
//!   (schemars' identical encoding for a one-level-nested object *and*
//!   a unit enum, confirmed by the same probe - the two are only told
//!   apart by following the ref into `definitions`, the distinction
//!   `skilj-graphql`'s own `resolve_field` makes server-side) - falls
//!   back to [`Widget::RawJson`], typed as raw JSON and substituted
//!   verbatim. Deliberately not following `$ref` here: real extra work
//!   for a niche win (a bare enum could instead render as a picker of
//!   its own), and the issue this module closes explicitly scopes
//!   nested shapes to this same fallback "at least initially".

use serde_json::{Map, Value};

/// One generated form field - `properties` order (alphabetical, since
/// this workspace's `serde_json` has no `preserve_order` feature - a
/// known, minor v1 limitation, not schema-declaration order).
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub widget: Widget,
}

#[derive(Debug, Clone)]
pub enum Widget {
    Text(String),
    Number(String),
    Bool(bool),
    RawJson(String),
}

/// Walks `schema`'s top-level `properties` into one [`Field`] each - see
/// this module's own doc comment for the classification rules. Every
/// widget starts at its zero value (`String::new()`, `false`) - nothing
/// pre-fills from the schema itself (no `default` support in v1, the
/// same scope cut `valid_filters`'s own `Other`-rejection design
/// document already made for a different JSON-Schema-touching feature:
/// cover the common case first). A schema with no `properties` object at
/// all (or one that fails to parse) yields an empty form, not an error -
/// `assemble_payload` on an empty `Vec<Field>` already produces the
/// valid `{}` a type with no fields expects.
pub fn fields_from_schema(schema: &str) -> Vec<Field> {
    let Ok(parsed) = serde_json::from_str::<Value>(schema) else {
        return Vec::new();
    };
    let Some(properties) = parsed.get("properties").and_then(Value::as_object) else {
        return Vec::new();
    };
    properties
        .iter()
        .map(|(name, property)| Field {
            name: name.clone(),
            widget: classify(property),
        })
        .collect()
}

fn classify(property: &Value) -> Widget {
    if property.get("$ref").is_some() {
        return Widget::RawJson(String::new());
    }
    match declared_type(property) {
        Some("string") => Widget::Text(String::new()),
        Some("integer") | Some("number") => Widget::Number(String::new()),
        Some("boolean") => Widget::Bool(false),
        _ => Widget::RawJson(String::new()),
    }
}

/// `property["type"]`, unwrapping the `["T", "null"]` shape `schemars`
/// renders `Option<T>` as - `None` for a missing `"type"` key entirely
/// (a bare `$ref` never has one; already handled by its own check
/// above) or a shape this function doesn't recognise.
fn declared_type(property: &Value) -> Option<&str> {
    match property.get("type") {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Array(candidates)) => candidates
            .iter()
            .find_map(|v| v.as_str().filter(|s| *s != "null")),
        _ => None,
    }
}

/// The inverse of [`fields_from_schema`] - builds the JSON object
/// `submitCommand`'s `payload`/an event's own creation path expects. A
/// blank [`Widget::Text`]/[`Widget::RawJson`] omits its key entirely
/// (mapping "left empty" to "not provided" for an optional field, rather
/// than sending an empty string or `null` the operator never typed) -
/// [`Widget::Bool`] always has a concrete state, so it's always
/// included. Returns `Err` with a human-readable message on the first
/// unparseable [`Widget::Number`]/[`Widget::RawJson`] field, naming
/// which field - blocking submission with an inline error rather than
/// sending malformed JSON the server would reject anyway with a less
/// specific one.
pub fn assemble_payload(fields: &[Field]) -> Result<Value, String> {
    let mut object = Map::new();
    for field in fields {
        match &field.widget {
            Widget::Text(value) => {
                if !value.is_empty() {
                    object.insert(field.name.clone(), Value::String(value.clone()));
                }
            }
            Widget::Number(value) => {
                if !value.is_empty() {
                    let number = value
                        .parse::<serde_json::Number>()
                        .map_err(|_| format!("{}: {value:?} is not a valid number", field.name))?;
                    object.insert(field.name.clone(), Value::Number(number));
                }
            }
            Widget::Bool(value) => {
                object.insert(field.name.clone(), Value::Bool(*value));
            }
            Widget::RawJson(value) => {
                if !value.is_empty() {
                    let parsed = serde_json::from_str::<Value>(value)
                        .map_err(|e| format!("{}: not valid JSON ({e})", field.name))?;
                    object.insert(field.name.clone(), parsed);
                }
            }
        }
    }
    Ok(Value::Object(object))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real `schema_for!` output for a struct with one field of each
    /// shape this module classifies - generated by the same throwaway
    /// probe crate used to verify the design, transcribed here rather
    /// than re-run at test time (no `schemars` dependency in this
    /// crate - deliberately, see this module's own doc comment on why
    /// a live schema is data already sitting on the type, not something
    /// this crate ever derives itself).
    const PROBE_SCHEMA: &str = r##"{
        "type": "object",
        "required": ["a", "c", "e", "f", "g", "h"],
        "properties": {
            "a": { "type": "string" },
            "b": { "type": ["string", "null"] },
            "c": { "type": "integer", "format": "int64" },
            "d": { "type": ["integer", "null"], "format": "int64" },
            "e": { "type": "boolean" },
            "f": { "$ref": "#/definitions/Status" },
            "g": { "$ref": "#/definitions/Nested" },
            "h": { "type": "array", "items": { "type": "string" } }
        }
    }"##;

    fn field<'a>(fields: &'a [Field], name: &str) -> &'a Field {
        fields
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("no field named {name}"))
    }

    #[test]
    fn a_required_string_becomes_a_text_widget() {
        let fields = fields_from_schema(PROBE_SCHEMA);
        assert!(matches!(field(&fields, "a").widget, Widget::Text(_)));
    }

    #[test]
    fn an_optional_string_also_becomes_a_text_widget() {
        let fields = fields_from_schema(PROBE_SCHEMA);
        assert!(matches!(field(&fields, "b").widget, Widget::Text(_)));
    }

    #[test]
    fn required_and_optional_integers_both_become_number_widgets() {
        let fields = fields_from_schema(PROBE_SCHEMA);
        assert!(matches!(field(&fields, "c").widget, Widget::Number(_)));
        assert!(matches!(field(&fields, "d").widget, Widget::Number(_)));
    }

    #[test]
    fn a_boolean_becomes_a_toggle_widget() {
        let fields = fields_from_schema(PROBE_SCHEMA);
        assert!(matches!(field(&fields, "e").widget, Widget::Bool(false)));
    }

    #[test]
    fn an_enum_ref_falls_back_to_raw_json() {
        let fields = fields_from_schema(PROBE_SCHEMA);
        assert!(matches!(field(&fields, "f").widget, Widget::RawJson(_)));
    }

    #[test]
    fn a_nested_object_ref_falls_back_to_raw_json() {
        let fields = fields_from_schema(PROBE_SCHEMA);
        assert!(matches!(field(&fields, "g").widget, Widget::RawJson(_)));
    }

    #[test]
    fn an_array_falls_back_to_raw_json() {
        let fields = fields_from_schema(PROBE_SCHEMA);
        assert!(matches!(field(&fields, "h").widget, Widget::RawJson(_)));
    }

    #[test]
    fn a_schema_with_no_properties_yields_an_empty_form() {
        assert!(fields_from_schema("{}").is_empty());
        assert!(fields_from_schema("not json").is_empty());
    }

    #[test]
    fn assemble_payload_round_trips_every_filled_widget() {
        let fields = vec![
            Field {
                name: "a".into(),
                widget: Widget::Text("hello".into()),
            },
            Field {
                name: "c".into(),
                widget: Widget::Number("42".into()),
            },
            Field {
                name: "e".into(),
                widget: Widget::Bool(true),
            },
            Field {
                name: "g".into(),
                widget: Widget::RawJson(r#"{"x":1}"#.into()),
            },
        ];
        let payload = assemble_payload(&fields).unwrap();
        assert_eq!(
            payload,
            serde_json::json!({ "a": "hello", "c": 42, "e": true, "g": { "x": 1 } })
        );
    }

    #[test]
    fn assemble_payload_omits_blank_text_and_raw_json_fields() {
        let fields = vec![
            Field {
                name: "a".into(),
                widget: Widget::Text(String::new()),
            },
            Field {
                name: "g".into(),
                widget: Widget::RawJson(String::new()),
            },
            Field {
                name: "e".into(),
                widget: Widget::Bool(false),
            },
        ];
        let payload = assemble_payload(&fields).unwrap();
        assert_eq!(payload, serde_json::json!({ "e": false }));
    }

    #[test]
    fn assemble_payload_rejects_an_unparseable_number_by_name() {
        let fields = vec![Field {
            name: "c".into(),
            widget: Widget::Number("not-a-number".into()),
        }];
        let err = assemble_payload(&fields).unwrap_err();
        assert!(
            err.contains('c'),
            "error should name the offending field: {err}"
        );
    }

    #[test]
    fn assemble_payload_rejects_invalid_raw_json_by_name() {
        let fields = vec![Field {
            name: "g".into(),
            widget: Widget::RawJson("{not json".into()),
        }];
        let err = assemble_payload(&fields).unwrap_err();
        assert!(
            err.contains('g'),
            "error should name the offending field: {err}"
        );
    }
}
