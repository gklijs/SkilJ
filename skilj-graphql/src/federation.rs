//! `/graphql` as an Apollo Federation v2 subgraph (docs/architecture.md
//! §194, `surface FederatedSubgraph` in specs/skilj.allium).
//!
//! Turned on with [`FederationOptions`], a builder choice rather than a
//! second endpoint. Every schema `/graphql` serves then:
//! - names its types and root fields under the options' prefix
//!   ([`crate::naming::Naming`]), so neither a second skilj service nor
//!   another subgraph's `Role` collides with them;
//! - marks the root fields a grant doesn't face at read or write level,
//!   and every type only they reach, `@inaccessible`: still callable
//!   directly, absent from the supergraph;
//! - makes each projection type an entity, keyed by `projectionKey`, and
//!   answers `_entities` through the same lookup and checks as
//!   `projection`.
//!
//! What a router composes is the *published* description
//! ([`published_sdl`]): the schema for the bounded contexts the options
//! publish, minus any made from a template, whoever asks. Queries still
//! run against the caller's own schema (docs/architecture.md §138).

use crate::naming::Naming;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, Schema, Type, TypeRef};
use async_graphql::parser::types::{
    BaseType, DocumentOperations, ExecutableDocument, Selection, ServiceDocument, TypeKind,
    TypeSystemDefinition,
};
use std::collections::{BTreeSet, HashMap, HashSet};

/// See the module doc comment. Built with [`FederationOptions::new`] and
/// handed to `skilj::SkiljBuilder::graphql_federation`.
#[derive(Clone, Debug, Default)]
pub struct FederationOptions {
    prefix: String,
    published: BTreeSet<String>,
}

impl FederationOptions {
    /// No prefix, nothing published: `/graphql` is a subgraph whose
    /// description names no bounded context.
    pub fn new() -> Self {
        Self::default()
    }

    /// The prefix every type and root field gets - see
    /// [`Naming::new`] for what it may be and how it's applied.
    /// `SkiljBuilder::build()` refuses an invalid one.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Names `bounded_context` in the published description. One made
    /// from a template is left out even when named here (`@guarantee
    /// TenantsAreNeverPublished`), and `build()` warns about it.
    pub fn publish(mut self, bounded_context: impl Into<String>) -> Self {
        self.published.insert(bounded_context.into());
        self
    }

    /// The bounded contexts [`publish`](Self::publish) named.
    pub fn published(&self) -> &BTreeSet<String> {
        &self.published
    }

    /// The naming the prefix makes, or why the prefix is invalid.
    pub fn naming(&self) -> Result<Naming, String> {
        Naming::new(&self.prefix)
    }
}

/// The root fields the description publishes, before the prefix: those a
/// grant faces at read or write level (`@guarantee
/// OnlyGrantFacingSurfacesArePublished`) - `ProjectionQuery`,
/// `EventSubscription` (with `epoch`, which resuming one needs),
/// `CommandSubmission`'s `submitCommand` and `PrivateFieldGrantManagement`.
/// Every other root field is `@inaccessible`.
pub const PUBLISHED_ROOT_FIELDS: &[&str] = &[
    "projection",
    "projectionSchema",
    "epoch",
    "listPrivateFieldGrants",
    "submitCommand",
    "grantPrivateFieldAccessForEvent",
    "grantPrivateFieldAccessForCommand",
    "revokePrivateFieldAccess",
    "allEvents",
    "eventsByType",
    "projectionUpdates",
];

/// Whether the root field skilj calls `name` (before the prefix) is
/// published.
pub fn is_published_root_field(name: &str) -> bool {
    PUBLISHED_ROOT_FIELDS.contains(&name)
}

/// The field a projection entity is keyed by: the instance key, as
/// `projection(key:)` takes it. Named so a state field is unlikely to
/// share it; one that does is left out of the type (see
/// `projection_types`).
pub const PROJECTION_KEY_FIELD: &str = "projectionKey";

/// The types no published root field reaches, by name, in a schema whose
/// root fields are flagged by [`is_published_root_field`] - given that
/// schema's SDL (without federation, so `Subscription` is in it) and its
/// naming. Composition refuses an `@inaccessible` type a published field
/// reaches, so this is computed rather than listed.
pub fn unreachable_from_published(sdl: &str, naming: &Naming) -> Result<HashSet<String>, String> {
    let document: ServiceDocument =
        async_graphql::parser::parse_schema(sdl).map_err(|e| e.to_string())?;
    let mut types: HashMap<String, &TypeKind> = HashMap::new();
    for definition in &document.definitions {
        if let TypeSystemDefinition::Type(ty) = definition {
            types.insert(ty.node.name.node.to_string(), &ty.node.kind);
        }
    }
    let published: HashSet<String> = PUBLISHED_ROOT_FIELDS
        .iter()
        .map(|name| naming.root(name))
        .collect();

    let mut reached: HashSet<String> = HashSet::new();
    let mut pending: Vec<String> = Vec::new();
    for root in ["Query", "Mutation", "Subscription"] {
        reached.insert(root.to_string());
        if let Some(TypeKind::Object(object)) = types.get(root) {
            for field in &object.fields {
                if published.contains(field.node.name.node.as_str()) {
                    pending.push(base_name(&field.node.ty.node.base));
                    pending.extend(
                        field
                            .node
                            .arguments
                            .iter()
                            .map(|arg| base_name(&arg.node.ty.node.base)),
                    );
                }
            }
        }
    }
    while let Some(name) = pending.pop() {
        if !reached.insert(name.clone()) {
            continue;
        }
        match types.get(&name) {
            Some(TypeKind::Object(object)) => {
                for field in &object.fields {
                    pending.push(base_name(&field.node.ty.node.base));
                    pending.extend(
                        field
                            .node
                            .arguments
                            .iter()
                            .map(|arg| base_name(&arg.node.ty.node.base)),
                    );
                }
            }
            Some(TypeKind::Interface(interface)) => {
                for field in &interface.fields {
                    pending.push(base_name(&field.node.ty.node.base));
                }
            }
            Some(TypeKind::Union(union)) => {
                pending.extend(union.members.iter().map(|m| m.node.to_string()));
            }
            Some(TypeKind::InputObject(input)) => {
                pending.extend(input.fields.iter().map(|f| base_name(&f.node.ty.node.base)));
            }
            _ => {}
        }
    }
    Ok(types
        .keys()
        .filter(|name| !reached.contains(*name) && !name.starts_with('_'))
        .cloned()
        .collect())
}

fn base_name(base: &BaseType) -> String {
    match base {
        BaseType::Named(name) => name.to_string(),
        BaseType::List(inner) => base_name(&inner.base),
    }
}

/// `ty` marked `@inaccessible` when its name is in `hidden`.
pub fn hide_if(ty: Type, hidden: &HashSet<String>) -> Type {
    match ty {
        Type::Object(o) if hidden.contains(o.type_name()) => Type::Object(o.inaccessible()),
        Type::Enum(e) if hidden.contains(e.type_name()) => Type::Enum(e.inaccessible()),
        Type::InputObject(i) if hidden.contains(i.type_name()) => {
            Type::InputObject(i.inaccessible())
        }
        Type::Union(u) if hidden.contains(u.type_name()) => Type::Union(u.inaccessible()),
        other => other,
    }
}

/// The description a router composes: `schema`'s federation SDL, plus its
/// `Subscription` type. async-graphql leaves the subscription type out of
/// a dynamic schema's federation SDL (it has no switch for it, unlike a
/// static schema's `enable_subscription_in_federation`), which would
/// leave a router unable to route `allEvents`, `eventsByType` or
/// `projectionUpdates` here; it's copied over from the plain SDL.
pub fn published_sdl(schema: &Schema) -> String {
    let mut sdl = schema.sdl_with_options(
        async_graphql::SDLExportOptions::new()
            .federation()
            .compose_directive(),
    );
    let plain = schema.sdl();
    if let Some(subscription) = type_block(&plain, "Subscription") {
        sdl.push('\n');
        sdl.push_str(subscription);
        sdl.push('\n');
    }
    sdl
}

/// The text of `type {name} { ... }` in an SDL async-graphql exported,
/// which puts each type's closing brace alone at the start of a line.
fn type_block<'a>(sdl: &'a str, name: &str) -> Option<&'a str> {
    let header = format!("type {name} {{\n");
    let start = if sdl.starts_with(&header) {
        0
    } else {
        sdl.find(&format!("\n{header}"))? + 1
    };
    let end = sdl[start..].find("\n}\n")? + start + 2;
    Some(&sdl[start..end])
}

/// Whether `query` asks for nothing but `_service` (and `__typename`):
/// the request a router or `rover subgraph introspect` sends for the
/// description, answered from [`service_schema`] instead of the caller's
/// own schema.
pub fn is_service_request(query: &str, operation_name: Option<&str>) -> bool {
    let Ok(document): Result<ExecutableDocument, _> = async_graphql::parser::parse_query(query)
    else {
        return false;
    };
    if !document.fragments.is_empty() {
        return false;
    }
    let operation = match &document.operations {
        DocumentOperations::Single(operation) => operation,
        DocumentOperations::Multiple(operations) => {
            match operation_name.and_then(|name| operations.get(name)) {
                Some(operation) => operation,
                None if operations.len() == 1 => operations.values().next().unwrap(),
                None => return false,
            }
        }
    };
    if operation.node.ty != async_graphql::parser::types::OperationType::Query {
        return false;
    }
    let items = &operation.node.selection_set.node.items;
    items
        .iter()
        .any(|item| matches!(&item.node, Selection::Field(f) if f.node.name.node == "_service"))
        && items.iter().all(|item| {
            matches!(&item.node, Selection::Field(f)
                if f.node.name.node == "_service" || f.node.name.node == "__typename")
        })
}

/// A schema of one field, `_service { sdl }`, answering with `sdl` - so a
/// `_service` request gets the published description whoever sends it,
/// with the caller's aliases, `__typename` and variables handled by
/// ordinary execution.
pub fn service_schema(sdl: String) -> Schema {
    let service =
        Object::new("_Service").field(Field::new("sdl", TypeRef::named(TypeRef::STRING), |ctx| {
            FieldFuture::new(async move {
                let sdl = ctx.parent_value.try_downcast_ref::<String>()?;
                Ok(Some(FieldValue::value(sdl.clone())))
            })
        }));
    let query = Object::new("Query").field(Field::new(
        "_service",
        TypeRef::named_nn("_Service"),
        move |_| {
            let sdl = sdl.clone();
            FieldFuture::new(async move { Ok(Some(FieldValue::owned_any(sdl))) })
        },
    ));
    Schema::build("Query", None, None)
        .register(service)
        .register(query)
        .finish()
        .expect("a fixed two-type schema")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_service_request_is_only_service_and_typename() {
        assert!(is_service_request("{ _service { sdl } }", None));
        assert!(is_service_request(
            "query SubgraphIntrospectQuery { _service { sdl } __typename }",
            None
        ));
        assert!(is_service_request("{ s: _service { sdl } }", None));
        assert!(!is_service_request("{ _service { sdl } epoch }", None));
        assert!(!is_service_request("{ epoch }", None));
        assert!(!is_service_request(
            "{ ...F } fragment F on Query { _service { sdl } }",
            None
        ));
        assert!(!is_service_request("mutation { _service { sdl } }", None));
        assert!(!is_service_request("not graphql", None));
        assert!(is_service_request(
            "query A { epoch } query B { _service { sdl } }",
            Some("B")
        ));
        assert!(!is_service_request(
            "query A { epoch } query B { _service { sdl } }",
            None
        ));
    }

    #[test]
    fn type_block_finds_a_whole_type() {
        let sdl =
            "type Query {\n\ta: Int\n}\n\ntype Subscription {\n\tb: Int\n\tc: Int\n}\n\nscalar X\n";
        assert_eq!(
            type_block(sdl, "Subscription"),
            Some("type Subscription {\n\tb: Int\n\tc: Int\n}")
        );
        assert_eq!(type_block(sdl, "Query"), Some("type Query {\n\ta: Int\n}"));
        assert_eq!(type_block(sdl, "Mutation"), None);
    }

    #[test]
    fn unreachable_types_are_those_only_admin_fields_reach() {
        let naming = Naming::new("ledger").unwrap();
        let sdl = r#"
            type Query {
                ledgerEpoch: String!
                ledgerProjectionSchema(name: String!): LedgerProjection
                ledgerBoundedContexts: [LedgerBoundedContext!]!
            }
            type Mutation {
                ledgerCreateRole(input: LedgerRoleInput!): LedgerRole!
            }
            type LedgerProjection { name: String! rebuild: LedgerRebuild }
            type LedgerRebuild { status: LedgerRebuildStatus! }
            enum LedgerRebuildStatus { PENDING }
            type LedgerBoundedContext { name: String! creator: LedgerRole }
            type LedgerRole { id: ID! }
            input LedgerRoleInput { name: String! }
        "#;
        let hidden = unreachable_from_published(sdl, &naming).unwrap();
        let mut hidden: Vec<_> = hidden.into_iter().collect();
        hidden.sort();
        assert_eq!(
            hidden,
            ["LedgerBoundedContext", "LedgerRole", "LedgerRoleInput"]
        );
    }
}
