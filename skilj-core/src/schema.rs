//! The payload schema model.
//!
//! The spec treats a schema as "an opaque JSON Schema string" checked by a
//! family of black boxes (`valid_schema`, `valid_payload`,
//! `valid_tag_mappings`, `valid_sensitive_fields`, `valid_private_fields`,
//! `valid_filters`, `schema_is_backwards_compatible`). Every one of those
//! needs the same structural understanding of the schema, so in practice the
//! black boxes are one parsed model. This module is that model: a subset of
//! JSON Schema (objects, scalars, arrays, `$ref` into `$defs`) that is enough
//! for the shapes the spec allows (scalar leaves, lists of scalars, one level
//! of reusable nesting) and for projection schemas, which the spec does not
//! constrain.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::error::{Error, Result};

/// JSON type of a scalar leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    String,
    Integer,
    Number,
    Boolean,
}

impl ScalarType {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "string" => ScalarType::String,
            "integer" => ScalarType::Integer,
            "number" => ScalarType::Number,
            "boolean" => ScalarType::Boolean,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            ScalarType::String => "string",
            ScalarType::Integer => "integer",
            ScalarType::Number => "number",
            ScalarType::Boolean => "boolean",
        }
    }

    pub fn accepts(self, v: &Value) -> bool {
        match self {
            ScalarType::String => v.is_string(),
            ScalarType::Integer => v.as_i64().is_some() || v.as_u64().is_some(),
            ScalarType::Number => v.is_number(),
            ScalarType::Boolean => v.is_boolean(),
        }
    }
}

/// A scalar leaf: its JSON type plus what the schema says it carries.
#[derive(Debug, Clone, PartialEq)]
pub struct Scalar {
    pub ty: ScalarType,
    /// `format` keyword. Used by the filter matrix: `ipv4`/`ipv6`/`ip`,
    /// `color`, `geo-point` gate `in_subnet`, `similar_color` and `near`.
    pub format: Option<String>,
    pub enum_values: Option<Vec<Value>>,
    /// Also accepts JSON null (`"type": ["string", "null"]`).
    pub nullable: bool,
}

/// One node of a schema.
#[derive(Debug, Clone, PartialEq)]
pub enum Shape {
    Scalar(Scalar),
    Array(Box<Shape>),
    Object(ObjectShape),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectShape {
    /// The `$defs` name when this object came in through a `$ref`.
    pub name: Option<String>,
    pub fields: BTreeMap<String, Field>,
    pub additional_properties: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub shape: Shape,
    pub required: bool,
}

/// What a dotted path resolved to.
#[derive(Debug, Clone, PartialEq)]
pub enum Leaf {
    Scalar(Scalar),
    ListOfScalars(Scalar),
}

impl Leaf {
    pub fn scalar(&self) -> &Scalar {
        match self {
            Leaf::Scalar(s) | Leaf::ListOfScalars(s) => s,
        }
    }
}

/// A parsed schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub root: ObjectShape,
    pub raw: Value,
}

impl Schema {
    /// `valid_schema`: well-formed JSON, a usable JSON Schema, and inside
    /// the subset this library understands. A schema this accepts is one
    /// `valid_payload` can use.
    pub fn parse(raw: &Value) -> Result<Schema> {
        let obj = raw
            .as_object()
            .ok_or_else(|| Error::invalid("schema must be a JSON object"))?;
        let defs = obj
            .get("$defs")
            .or_else(|| obj.get("definitions"))
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let shape = parse_shape(raw, &defs, 0)?;
        match shape {
            Shape::Object(root) => Ok(Schema {
                root,
                raw: raw.clone(),
            }),
            _ => Err(Error::invalid("schema root must be an object")),
        }
    }

    pub fn parse_str(s: &str) -> Result<Schema> {
        let v: Value = serde_json::from_str(s)
            .map_err(|e| Error::invalid(format!("schema is not JSON: {e}")))?;
        Schema::parse(&v)
    }

    /// `valid_payload`: does this payload conform to the schema.
    pub fn validate(&self, payload: &Value) -> Result<()> {
        validate_object(&self.root, payload, "")
    }

    /// Resolve a bare name or two-segment dotted path to a scalar or
    /// list-of-scalars leaf (the `valid_tag_mappings`/`valid_sensitive_fields`
    /// existence check). A nested-object-typed field is not a target.
    pub fn resolve(&self, path: &str) -> Result<Leaf> {
        let segments: Vec<&str> = path.split('.').collect();
        if segments.is_empty() || segments.len() > 2 || segments.iter().any(|s| s.is_empty()) {
            return Err(Error::invalid(format!(
                "field path '{path}' must be a name or a two-segment dotted path"
            )));
        }
        let first =
            self.root.fields.get(segments[0]).ok_or_else(|| {
                Error::invalid(format!("schema declares no field '{}'", segments[0]))
            })?;
        let target = if segments.len() == 2 {
            match &first.shape {
                Shape::Object(o) => o
                    .fields
                    .get(segments[1])
                    .ok_or_else(|| Error::invalid(format!("schema declares no field '{path}'")))?,
                _ => {
                    return Err(Error::invalid(format!(
                        "'{}' is not a nested object, cannot address '{path}'",
                        segments[0]
                    )))
                }
            }
        } else {
            first
        };
        match &target.shape {
            Shape::Scalar(s) => Ok(Leaf::Scalar(s.clone())),
            Shape::Array(inner) => match inner.as_ref() {
                Shape::Scalar(s) => Ok(Leaf::ListOfScalars(s.clone())),
                _ => Err(Error::invalid(format!("'{path}' is a list of non-scalars"))),
            },
            Shape::Object(_) => Err(Error::invalid(format!(
                "'{path}' is a nested object; only a scalar leaf inside it can be addressed"
            ))),
        }
    }

    /// `schema_is_backwards_compatible(existing, self)`, top-level fields
    /// only: none removed, none tightened to required, new ones optional,
    /// declared type never changes.
    pub fn is_backwards_compatible_with(&self, existing: &Schema) -> Result<()> {
        for (name, old) in &existing.root.fields {
            let new = self
                .root
                .fields
                .get(name)
                .ok_or_else(|| Error::invalid(format!("field '{name}' was removed")))?;
            if !old.required && new.required {
                return Err(Error::invalid(format!("field '{name}' became required")));
            }
            if !same_declared_type(&old.shape, &new.shape) {
                return Err(Error::invalid(format!("field '{name}' changed type")));
            }
        }
        for (name, new) in &self.root.fields {
            if !existing.root.fields.contains_key(name) && new.required {
                return Err(Error::invalid(format!(
                    "new field '{name}' must be optional"
                )));
            }
        }
        Ok(())
    }
}

fn same_declared_type(a: &Shape, b: &Shape) -> bool {
    match (a, b) {
        (Shape::Scalar(x), Shape::Scalar(y)) => x.ty == y.ty,
        (Shape::Array(x), Shape::Array(y)) => same_declared_type(x, y),
        // A nested shape's internal evolution is not tracked here (spec):
        // it stays "an object".
        (Shape::Object(_), Shape::Object(_)) => true,
        _ => false,
    }
}

const MAX_DEPTH: usize = 8;

fn parse_shape(v: &Value, defs: &Map<String, Value>, depth: usize) -> Result<Shape> {
    if depth > MAX_DEPTH {
        return Err(Error::invalid("schema nests too deeply"));
    }
    let obj = v
        .as_object()
        .ok_or_else(|| Error::invalid("schema node must be an object"))?;
    if let Some(r) = obj.get("$ref") {
        let r = r
            .as_str()
            .ok_or_else(|| Error::invalid("$ref must be a string"))?;
        let name = r
            .strip_prefix("#/$defs/")
            .or_else(|| r.strip_prefix("#/definitions/"))
            .ok_or_else(|| Error::invalid(format!("unsupported $ref '{r}'")))?;
        let target = defs
            .get(name)
            .ok_or_else(|| Error::invalid(format!("$ref to undefined '{name}'")))?;
        let mut shape = parse_shape(target, defs, depth + 1)?;
        if let Shape::Object(o) = &mut shape {
            o.name = Some(name.to_string());
        }
        return Ok(shape);
    }
    let (ty, nullable) = match obj.get("type") {
        Some(Value::String(s)) => (s.clone(), false),
        Some(Value::Array(types)) => {
            let names: Vec<&str> = types.iter().filter_map(Value::as_str).collect();
            let non_null: Vec<&str> = names.iter().copied().filter(|t| *t != "null").collect();
            if non_null.len() != 1 || names.len() != types.len() {
                return Err(Error::invalid(
                    "a type list may only add \"null\" to one type",
                ));
            }
            (non_null[0].to_string(), names.contains(&"null"))
        }
        Some(_) => return Err(Error::invalid("type must be a string or list")),
        None if obj.contains_key("properties") => ("object".to_string(), false),
        None if obj.contains_key("enum") => ("string".to_string(), false),
        None => return Err(Error::invalid("schema node has no type")),
    };
    match ty.as_str() {
        "object" => {
            let props = match obj.get("properties") {
                Some(Value::Object(p)) => p.clone(),
                Some(_) => return Err(Error::invalid("properties must be an object")),
                None => Map::new(),
            };
            let required: Vec<String> = match obj.get("required") {
                Some(Value::Array(r)) => r
                    .iter()
                    .map(|x| {
                        x.as_str()
                            .map(str::to_string)
                            .ok_or_else(|| Error::invalid("required must list strings"))
                    })
                    .collect::<Result<_>>()?,
                Some(_) => return Err(Error::invalid("required must be an array")),
                None => vec![],
            };
            for r in &required {
                if !props.contains_key(r) {
                    return Err(Error::invalid(format!(
                        "required names undeclared field '{r}'"
                    )));
                }
            }
            let mut fields = BTreeMap::new();
            for (name, sub) in props {
                if name.contains('.') {
                    return Err(Error::invalid(format!(
                        "field name '{name}' may not contain '.'"
                    )));
                }
                let shape = parse_shape(&sub, defs, depth + 1)?;
                fields.insert(
                    name.clone(),
                    Field {
                        shape,
                        required: required.contains(&name),
                    },
                );
            }
            let additional_properties =
                !matches!(obj.get("additionalProperties"), Some(Value::Bool(false)));
            Ok(Shape::Object(ObjectShape {
                name: None,
                fields,
                additional_properties,
            }))
        }
        "array" => {
            let items = obj
                .get("items")
                .ok_or_else(|| Error::invalid("array schema needs items"))?;
            Ok(Shape::Array(Box::new(parse_shape(items, defs, depth + 1)?)))
        }
        other => {
            let ty = ScalarType::parse(other)
                .ok_or_else(|| Error::invalid(format!("unsupported type '{other}'")))?;
            let enum_values = match obj.get("enum") {
                Some(Value::Array(a)) => Some(a.clone()),
                Some(_) => return Err(Error::invalid("enum must be an array")),
                None => None,
            };
            let format = obj
                .get("format")
                .and_then(Value::as_str)
                .map(str::to_string);
            Ok(Shape::Scalar(Scalar {
                ty,
                format,
                enum_values,
                nullable,
            }))
        }
    }
}

fn validate_object(shape: &ObjectShape, v: &Value, at: &str) -> Result<()> {
    let obj = v
        .as_object()
        .ok_or_else(|| Error::invalid(format!("{} must be an object", display_at(at))))?;
    for (name, field) in &shape.fields {
        let here = join(at, name);
        match obj.get(name) {
            None => {
                if field.required {
                    return Err(Error::invalid(format!("missing required field '{here}'")));
                }
            }
            Some(value) => validate_shape(&field.shape, value, &here)?,
        }
    }
    if !shape.additional_properties {
        for k in obj.keys() {
            if !shape.fields.contains_key(k) {
                return Err(Error::invalid(format!(
                    "unexpected field '{}'",
                    join(at, k)
                )));
            }
        }
    }
    Ok(())
}

fn validate_shape(shape: &Shape, v: &Value, at: &str) -> Result<()> {
    match shape {
        Shape::Scalar(s) => {
            if v.is_null() && s.nullable {
                return Ok(());
            }
            if !s.ty.accepts(v) {
                return Err(Error::invalid(format!("'{at}' must be a {}", s.ty.name())));
            }
            if let Some(allowed) = &s.enum_values {
                if !allowed.contains(v) {
                    return Err(Error::invalid(format!(
                        "'{at}' is not one of the allowed values"
                    )));
                }
            }
            Ok(())
        }
        Shape::Array(inner) => {
            let items = v
                .as_array()
                .ok_or_else(|| Error::invalid(format!("'{at}' must be an array")))?;
            for (i, item) in items.iter().enumerate() {
                validate_shape(inner, item, &format!("{at}[{i}]"))?;
            }
            Ok(())
        }
        Shape::Object(o) => validate_object(o, v, at),
    }
}

fn join(at: &str, name: &str) -> String {
    if at.is_empty() {
        name.to_string()
    } else {
        format!("{at}.{name}")
    }
}

fn display_at(at: &str) -> String {
    if at.is_empty() {
        "payload".to_string()
    } else {
        format!("'{at}'")
    }
}

/// Read the value a bare or dotted path points at in a payload.
/// `None` when the path (or its enclosing object) is absent.
pub fn read_path<'a>(payload: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = payload;
    for seg in path.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

/// Mutable access to the leaf a path names, when present.
pub fn read_path_mut<'a>(payload: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    let mut cur = payload;
    for seg in path.split('.') {
        cur = cur.as_object_mut()?.get_mut(seg)?;
    }
    Some(cur)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn account() -> Schema {
        Schema::parse(&json!({
            "type": "object",
            "properties": {
                "account_id": {"type": "string"},
                "amount": {"type": "integer"},
                "labels": {"type": "array", "items": {"type": "string"}},
                "address": {"$ref": "#/$defs/Address"}
            },
            "required": ["account_id", "amount"],
            "$defs": {"Address": {"type": "object", "properties": {"country": {"type": "string"}}}}
        }))
        .unwrap()
    }

    #[test]
    fn validates_payloads() {
        let s = account();
        s.validate(&json!({"account_id": "a", "amount": 5}))
            .unwrap();
        assert!(s.validate(&json!({"account_id": "a"})).is_err());
        assert!(s
            .validate(&json!({"account_id": "a", "amount": 1.5}))
            .is_err());
        s.validate(&json!({"account_id": "a", "amount": 1, "address": {"country": "NL"}}))
            .unwrap();
    }

    #[test]
    fn resolves_paths() {
        let s = account();
        assert!(matches!(s.resolve("account_id").unwrap(), Leaf::Scalar(_)));
        assert!(matches!(
            s.resolve("labels").unwrap(),
            Leaf::ListOfScalars(_)
        ));
        assert!(matches!(
            s.resolve("address.country").unwrap(),
            Leaf::Scalar(_)
        ));
        assert!(s.resolve("address").is_err());
        assert!(s.resolve("address.country.x").is_err());
        assert!(s.resolve("nope").is_err());
    }

    #[test]
    fn compatibility() {
        let old = account();
        let added_optional = Schema::parse(&json!({
            "type": "object",
            "properties": {
                "account_id": {"type": "string"}, "amount": {"type": "integer"},
                "labels": {"type": "array", "items": {"type": "string"}},
                "address": {"type": "object", "properties": {}},
                "note": {"type": "string"}
            },
            "required": ["account_id"]
        }))
        .unwrap();
        added_optional.is_backwards_compatible_with(&old).unwrap();
        let retyped = Schema::parse(&json!({
            "type": "object",
            "properties": {
                "account_id": {"type": "string"}, "amount": {"type": "number"},
                "labels": {"type": "array", "items": {"type": "string"}},
                "address": {"type": "object", "properties": {}}
            },
            "required": ["account_id", "amount"]
        }))
        .unwrap();
        assert!(retyped.is_backwards_compatible_with(&old).is_err());
    }
}
