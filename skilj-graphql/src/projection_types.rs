//! Generates one GraphQL `Object` type per registered `Projection`
//! (`Projection.schema`, a `schemars`-derived JSON Schema string) plus a
//! `ProjectionResult` union over all of them - `ProjectionQuery`'s own
//! wire shape (docs/architecture.md §5.1, and the plan at
//! `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`). See
//! `resolvers::projection_query` for the one field that resolves into
//! this union, the same `FieldValue::owned_any(v).with_type(name)`
//! pattern `resolvers::token_revocation`'s `AccessToken` union already
//! uses.
//!
//! Verified against a real `schemars::schema_for!` output (0.8, this
//! workspace's pinned version): a nested struct field is
//! `{"$ref": "#/definitions/Name"}`, resolved against the schema's own
//! *top-level* `"definitions"` map regardless of nesting depth (never a
//! second, nested `definitions` map); an optional scalar's `type` may
//! additionally appear as `["T","null"]`, but `required` array absence
//! is the authoritative nullability signal this module actually uses -
//! matching `skilj-core::event_store::schema_required`'s own convention,
//! reimplemented here rather than exposed from that module (a `pub`
//! widening purely to serve a type-generation concern `event_store`
//! itself has nothing to do with); a list is
//! `{"type":"array","items":{"type":"T"}}`.
//!
//! See the payload schema shape note above `entity CommandType` in
//! specs/skilj.allium (~line 332) for the contract this mapping assumes:
//! a scalar (optionally nullable), a list of scalars, or exactly one
//! level of a named nested shape whose own fields are scalars/lists of
//! scalars only - never a list of nested objects, never a second level
//! of nesting. That cap is stated but *not enforced* anywhere upstream
//! (the spec's own words: a schema outside it is "undefined rather than
//! defined-and-forbidden") - so a field this module doesn't recognise
//! (deeper nesting, a list of objects, an unrecognised `type`) renders
//! as an opaque JSON-encoded `String` field instead of panicking
//! `graphql_router()` for the whole server over one misbehaving
//! projection elsewhere. `$ref` resolution only ever happens at
//! `depth == 0` for the identical reason - refusing to recurse a second
//! level, rather than trusting a schema to actually honour the cap,
//! guards against runaway/cyclic `$ref` chains a hostile or buggy schema
//! could otherwise contain.
//!
//! Every generated field's `ctx.parent_value` downcasts to
//! `serde_json::Value` - the projection's own decoded state - both at
//! the top level and the one nested level, rather than `gql_types.rs`'s
//! per-static-Rust-type generics: there is no static Rust type behind
//! any of this, since a projection's `T::State` may not even exist as a
//! compiled Rust type in this process (see the doc comment above
//! `plugin::ProjectionDispatcher::default_state`) - the schema string is
//! all this module ever has.

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, TypeRef, Union};
use async_graphql::Value;
use serde_json::Map;
use skilj_core::db::{self, Pool};

/// The GraphQL type name one registered projection's state renders as -
/// shared by `build` (which registers it) and `resolvers::projection_query`
/// (which tags its `FieldValue` with it), so both always agree. Bounded
/// context names are already constrained to a safe identifier pattern
/// (`bootstrap::valid_bounded_context_name`); projection names aren't -
/// any more than `EventType`/`CommandType` names already aren't - so a
/// projection registered with GraphQL-unsafe characters in its name is a
/// pre-existing class of gap this doesn't newly introduce.
pub fn graphql_type_name(bounded_context: &str, projection_name: &str) -> String {
    format!("{bounded_context}_{projection_name}")
}

/// Every registered projection's generated `Object` (including any
/// nested-shape `Object`s it needed), plus the `ProjectionResult` union
/// over every top-level one - `None` when nothing anywhere parsed to at
/// least one usable member. An empty union isn't valid to register, and
/// "nothing to offer" is a legitimate startup state - a fresh app with
/// no projections registered yet, not an error.
pub async fn build(pool: &Pool) -> skilj_core::error::Result<Option<(Vec<Object>, Union)>> {
    let mut objects = Vec::new();
    let mut union = Union::new("ProjectionResult");
    let mut any = false;

    for bc in db::list_bounded_contexts(pool).await? {
        for projection in db::list_projections_for_bounded_context(pool, &bc.name).await? {
            let type_name = graphql_type_name(&bc.name, &projection.name);
            let Some(root): Option<serde_json::Value> =
                serde_json::from_str(&projection.schema).ok()
            else {
                eprintln!(
                    "skilj: projection {:?}/{:?} has an unparseable schema - excluded from \
                     ProjectionQuery this run",
                    bc.name, projection.name
                );
                continue;
            };
            let mut extra = Vec::new();
            let Some(object) = object_from_schema_value(&type_name, &root, &root, 0, &mut extra)
            else {
                eprintln!(
                    "skilj: projection {:?}/{:?} has no usable top-level properties - excluded \
                     from ProjectionQuery this run",
                    bc.name, projection.name
                );
                continue;
            };
            union = union.possible_type(type_name);
            objects.push(object);
            objects.extend(extra);
            any = true;
        }
    }

    if !any {
        return Ok(None);
    }
    Ok(Some((objects, union)))
}

/// `schema` describes the object being built right now (the whole
/// top-level schema at `depth == 0`, or one named `definitions` entry at
/// `depth == 1`); `root` is always the *whole* top-level schema, since
/// `$ref` targets always live in its `definitions` map regardless of how
/// deep the field referencing them is nested.
fn object_from_schema_value(
    type_name: &str,
    schema: &serde_json::Value,
    root: &serde_json::Value,
    depth: u8,
    extra_objects: &mut Vec<Object>,
) -> Option<Object> {
    let properties = schema.get("properties")?.as_object()?;
    if properties.is_empty() {
        return None;
    }
    let required = required_fields(schema);
    let definitions = root.get("definitions").and_then(|v| v.as_object());

    let mut object = Object::new(type_name.to_string());
    for (field_name, field_schema) in properties {
        let nullable = !required.contains(field_name);
        let field = build_field(
            type_name,
            field_name,
            field_schema,
            nullable,
            root,
            definitions,
            depth,
            extra_objects,
        );
        object = object.field(field);
    }
    Some(object)
}

fn required_fields(schema: &serde_json::Value) -> Vec<String> {
    schema
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// A JSON Schema `"type"` value as one string - handles both the plain
/// `"string"` shape and the `["string","null"]` shape an `Option<T>`
/// field's `type` may carry (nullability itself is decided from
/// `required` instead - see this module's own doc comment - this only
/// recovers the *scalar* kind either shape names).
fn json_type_str(type_value: &serde_json::Value) -> Option<&str> {
    match type_value {
        serde_json::Value::String(s) => Some(s.as_str()),
        serde_json::Value::Array(arr) => {
            arr.iter().find_map(|v| v.as_str().filter(|s| *s != "null"))
        }
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum ScalarKind {
    String,
    Int,
    Float,
    Boolean,
}

fn scalar_kind_and_name(type_str: &str) -> Option<(ScalarKind, &'static str)> {
    match type_str {
        "string" => Some((ScalarKind::String, TypeRef::STRING)),
        "integer" => Some((ScalarKind::Int, TypeRef::INT)),
        "number" => Some((ScalarKind::Float, TypeRef::FLOAT)),
        "boolean" => Some((ScalarKind::Boolean, TypeRef::BOOLEAN)),
        _ => None,
    }
}

fn scalar_json_to_value(raw: &serde_json::Value, kind: ScalarKind) -> Value {
    match kind {
        ScalarKind::String => raw
            .as_str()
            .map(|s| Value::from(s.to_string()))
            .unwrap_or(Value::Null),
        ScalarKind::Int => raw.as_i64().map(Value::from).unwrap_or(Value::Null),
        ScalarKind::Float => raw.as_f64().map(Value::from).unwrap_or(Value::Null),
        ScalarKind::Boolean => raw.as_bool().map(Value::from).unwrap_or(Value::Null),
    }
}

#[derive(Clone)]
enum FieldKind {
    Scalar(ScalarKind),
    ScalarList(ScalarKind),
    NestedObject,
    /// A field shape this module doesn't recognise - see the module doc
    /// comment. Rendered as compact JSON text rather than dropped, so a
    /// caller sees *something* rather than a silently missing field.
    OpaqueJson,
}

fn wrap(nullable: bool, name: String, list: bool) -> TypeRef {
    match (nullable, list) {
        (true, false) => TypeRef::named(name),
        (false, false) => TypeRef::named_nn(name),
        (true, true) => TypeRef::named_nn_list(name),
        (false, true) => TypeRef::named_nn_list_nn(name),
    }
}

/// snake_case → camelCase, matching what `async-graphql`'s own derive
/// macros already do by default (§5.1) - no new dependency for
/// something this mechanical.
fn snake_to_camel(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;
    for ch in s.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.extend(ch.to_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn build_field(
    parent_type_name: &str,
    field_name: &str,
    field_schema: &serde_json::Value,
    nullable: bool,
    root: &serde_json::Value,
    definitions: Option<&Map<String, serde_json::Value>>,
    depth: u8,
    extra_objects: &mut Vec<Object>,
) -> Field {
    let gql_name = snake_to_camel(field_name);
    let json_key = field_name.to_string();

    // One level of a named nested shape - only ever resolved starting
    // from the top level (depth == 0); see the module doc comment for
    // why a second level falls through to the opaque fallback instead of
    // recursing.
    if depth == 0 {
        if let Some(def_name) = field_schema
            .get("$ref")
            .and_then(|v| v.as_str())
            .and_then(|r| r.rsplit('/').next())
        {
            if let Some(def_schema) = definitions.and_then(|defs| defs.get(def_name)) {
                let nested_type_name = format!("{parent_type_name}_{field_name}");
                if let Some(nested_object) = object_from_schema_value(
                    &nested_type_name,
                    def_schema,
                    root,
                    depth + 1,
                    extra_objects,
                ) {
                    extra_objects.push(nested_object);
                    let ty = wrap(nullable, nested_type_name, false);
                    return dynamic_field(gql_name, ty, json_key, FieldKind::NestedObject);
                }
            }
        }
    }

    let type_value = field_schema.get("type");

    if type_value.and_then(json_type_str) == Some("array") {
        if let Some((kind, scalar_name)) = field_schema
            .get("items")
            .and_then(|items| items.get("type"))
            .and_then(json_type_str)
            .and_then(scalar_kind_and_name)
        {
            let ty = wrap(nullable, scalar_name.to_string(), true);
            return dynamic_field(gql_name, ty, json_key, FieldKind::ScalarList(kind));
        }
    } else if let Some(type_str) = type_value.and_then(json_type_str) {
        if let Some((kind, scalar_name)) = scalar_kind_and_name(type_str) {
            let ty = wrap(nullable, scalar_name.to_string(), false);
            return dynamic_field(gql_name, ty, json_key, FieldKind::Scalar(kind));
        }
    }

    // Unsupported shape - opaque JSON fallback, not a panic.
    let ty = wrap(nullable, TypeRef::STRING.to_string(), false);
    dynamic_field(gql_name, ty, json_key, FieldKind::OpaqueJson)
}

fn dynamic_field(gql_name: String, ty: TypeRef, json_key: String, kind: FieldKind) -> Field {
    Field::new(gql_name, ty, move |ctx| {
        let json_key = json_key.clone();
        let kind = kind.clone();
        FieldFuture::new(async move {
            let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
            Ok(render_field(parent.get(&json_key), &kind))
        })
    })
}

fn render_field<'a>(raw: Option<&serde_json::Value>, kind: &FieldKind) -> Option<FieldValue<'a>> {
    let raw = raw?;
    if raw.is_null() {
        return None;
    }
    match kind {
        FieldKind::Scalar(scalar) => Some(FieldValue::value(scalar_json_to_value(raw, *scalar))),
        FieldKind::ScalarList(scalar) => {
            let items = raw
                .as_array()?
                .iter()
                .map(|v| FieldValue::value(scalar_json_to_value(v, *scalar)));
            Some(FieldValue::list(items))
        }
        // No `.with_type(...)` here - unlike the top-level `projection`
        // field's own `ProjectionResult` union dispatch
        // (`resolvers::projection_query`), a nested-object field's
        // declared GraphQL type is already the one concrete `Object`
        // `build_field` generated for it - there's no polymorphic choice
        // left for a resolver to disambiguate at runtime.
        FieldKind::NestedObject => Some(FieldValue::owned_any(raw.clone())),
        FieldKind::OpaqueJson => Some(FieldValue::value(Value::from(raw.to_string()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_graphql::dynamic::Schema;

    /// A real `schemars::schema_for!` output (0.8) for a struct with a
    /// required `i64`, an optional `String`, a `Vec<String>`, and a
    /// nested struct field - captured directly from running
    /// `schemars::schema_for!` against that exact shape (see this
    /// module's own doc comment), not hand-written, so this test exerts
    /// the real shape the mapping has to handle, not an idealised one.
    const ACCOUNT_BALANCE_SCHEMA: &str = r##"{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "AccountBalanceState",
        "type": "object",
        "required": ["address", "tags", "total"],
        "properties": {
            "address": { "$ref": "#/definitions/Address" },
            "label": { "type": ["string", "null"] },
            "tags": { "type": "array", "items": { "type": "string" } },
            "total": { "type": "integer", "format": "int64" }
        },
        "definitions": {
            "Address": {
                "type": "object",
                "required": ["country"],
                "properties": {
                    "country": { "type": "string" },
                    "zip": { "type": ["string", "null"] }
                }
            }
        }
    }"##;

    /// Builds a throwaway one-field `Query` around a single generated
    /// projection object, so the whole scalar/nullable/list/nested-object
    /// pipeline can be exercised via a real GraphQL execution rather than
    /// reaching into `Object`/`Field` internals.
    async fn execute_against(
        type_name: &str,
        schema_json: &str,
        state_json: &str,
        query: &str,
    ) -> async_graphql::Value {
        let root: serde_json::Value = serde_json::from_str(schema_json).unwrap();
        let mut extra = Vec::new();
        let object = object_from_schema_value(type_name, &root, &root, 0, &mut extra)
            .expect("test schema always has usable top-level properties");

        let state: serde_json::Value = serde_json::from_str(state_json).unwrap();
        // No `.with_type(...)` here, unlike the real resolver
        // (`resolvers::projection_query`) - that's only meaningful when
        // the field's declared type is a union/interface being resolved
        // polymorphically; `value` here is declared as the concrete
        // generated `Object` directly, so a plain `owned_any` is correct.
        let query_object = Object::new("Query").field(Field::new(
            "value",
            TypeRef::named_nn(type_name),
            move |_ctx| {
                let state = state.clone();
                FieldFuture::new(async move { Ok(Some(FieldValue::owned_any(state))) })
            },
        ));

        let mut builder = Schema::build(query_object.type_name(), None, None)
            .register(query_object)
            .register(object);
        for nested in extra {
            builder = builder.register(nested);
        }
        let schema = builder.finish().expect("test schema is well-formed");

        let response = schema.execute(query).await;
        assert!(
            response.errors.is_empty(),
            "GraphQL errors: {:?}",
            response.errors
        );
        response.data
    }

    #[test]
    fn scalar_nullable_list_and_nested_object_all_map_correctly() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let data = execute_against(
                "Banking_AccountBalance",
                ACCOUNT_BALANCE_SCHEMA,
                r#"{"total":25,"label":null,"tags":["a","b"],"address":{"country":"NL","zip":null}}"#,
                "{ value { total label tags address { country zip } } }",
            )
            .await;
            assert_eq!(
                data,
                async_graphql::value!({
                    "value": {
                        "total": 25,
                        "label": null,
                        "tags": ["a", "b"],
                        "address": { "country": "NL", "zip": null }
                    }
                })
            );
        });
    }

    #[test]
    fn a_required_field_omitted_from_the_selection_set_does_not_break_the_rest() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let data = execute_against(
                "Banking_AccountBalance",
                ACCOUNT_BALANCE_SCHEMA,
                r#"{"total":7,"tags":[],"address":{"country":"BE"}}"#,
                "{ value { total tags } }",
            )
            .await;
            assert_eq!(
                data,
                async_graphql::value!({ "value": { "total": 7, "tags": [] } })
            );
        });
    }

    #[test]
    fn an_unrecognised_field_shape_falls_back_to_an_opaque_json_string() {
        // A list of nested objects - explicitly out of scope per the
        // payload schema shape note (specs/skilj.allium, ~line 341: "a
        // nested shape may only be the type of a single field, never the
        // element type of a list") - must not panic schema generation.
        const SCHEMA: &str = r##"{
            "type": "object",
            "required": ["items"],
            "properties": {
                "items": { "type": "array", "items": { "$ref": "#/definitions/Item" } }
            },
            "definitions": {
                "Item": { "type": "object", "properties": { "n": { "type": "integer" } } }
            }
        }"##;
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let data = execute_against(
                "Weird_Projection",
                SCHEMA,
                r#"{"items":[{"n":1},{"n":2}]}"#,
                "{ value { items } }",
            )
            .await;
            assert_eq!(
                data,
                async_graphql::value!({ "value": { "items": r#"[{"n":1},{"n":2}]"# } })
            );
        });
    }

    #[test]
    fn a_schema_with_no_top_level_properties_is_skipped() {
        assert!(object_from_schema_value(
            "Empty_Projection",
            &serde_json::json!({ "type": "object", "properties": {} }),
            &serde_json::json!({ "type": "object", "properties": {} }),
            0,
            &mut Vec::new(),
        )
        .is_none());
    }

    #[test]
    fn snake_to_camel_converts_multi_word_field_names() {
        assert_eq!(snake_to_camel("total"), "total");
        assert_eq!(snake_to_camel("account_total"), "accountTotal");
        assert_eq!(snake_to_camel("a_b_c"), "aBC");
    }
}
