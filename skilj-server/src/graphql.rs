//! The GraphQL track, generated from registered state.
//!
//! The wire contract is deferred by the spec; this is our pass at it:
//!
//! * Queries are namespaced per bounded context: `{ bank { Balance(key: "a1") { balance } } }`.
//!   A context the caller holds no grant on answers as not existing.
//! * Mutations are flat and prefixed (`bank_openAccount`, `bank_mintToken`) so
//!   they run serially, as GraphQL runs root mutation fields.
//! * Subscriptions: `bank_events(...)` (all-events) and `bank_<EventType>(filters)`.
//! * Projections are typed from Projection.schema; commands get typed input
//!   objects from CommandType.schema. Event payloads stay JSON: a sensitive
//!   leaf's stored type is a ciphertext string whatever the schema says.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_graphql::dynamic::*;
use async_graphql::{ErrorExtensions, Value as GqlValue};
use futures::StreamExt;
use serde::Serialize;
use serde_json::{json, Value};
use skilj_core::engine::{CommandOutcome, CommandRequest, Registry, Rendered, SubscriptionEnd};
use skilj_core::model::*;
use skilj_core::schema::{Schema as PSchema, Shape};
use skilj_core::{Error, Skilj};
use tokio::sync::RwLock;

use crate::auth::Identity;

/// Per-request data.
#[derive(Clone)]
pub struct Ctx {
    pub skilj: Skilj,
    pub identity: Option<Identity>,
}

pub fn gql_err(e: Error) -> async_graphql::Error {
    let code = e.code();
    let msg = match &e {
        Error::Database(_) => {
            tracing::error!("{e}");
            "internal error".to_string()
        }
        _ => e.to_string(),
    };
    async_graphql::Error::new(msg).extend_with(|_, ext| ext.set("code", code))
}

fn to_gql(v: Value) -> GqlValue {
    GqlValue::from_json(v).unwrap_or(GqlValue::Null)
}

fn ser<T: Serialize>(t: &T) -> GqlValue {
    to_gql(serde_json::to_value(t).unwrap_or(Value::Null))
}

fn ctx_of<'a>(ctx: &'a ResolverContext<'_>) -> async_graphql::Result<&'a Ctx> {
    ctx.data::<Ctx>()
}

fn identity(ctx: &ResolverContext<'_>) -> async_graphql::Result<(Skilj, String)> {
    let c = ctx_of(ctx)?;
    let id = c
        .identity
        .as_ref()
        .ok_or_else(|| gql_err(Error::forbidden("authentication required")))?;
    Ok((c.skilj.clone(), id.subject.clone()))
}

async fn grant(ctx: &ResolverContext<'_>, bc: &str) -> async_graphql::Result<Grant> {
    let (s, sub) = identity(ctx)?;
    s.resolve_grant(&sub, bc).await.map_err(gql_err)
}

async fn superadmin(ctx: &ResolverContext<'_>) -> async_graphql::Result<(Skilj, Role)> {
    let (s, sub) = identity(ctx)?;
    let r = s.resolve_superadmin(&sub).await.map_err(gql_err)?;
    Ok((s, r))
}

fn pascal(s: &str) -> String {
    s.split(['_', '-'])
        .filter(|p| !p.is_empty())
        .map(|p| {
            let mut c = p.chars();
            match c.next() {
                Some(f) => f.to_ascii_uppercase().to_string() + c.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn lower_camel(s: &str) -> String {
    let p = pascal(s);
    let mut c = p.chars();
    match c.next() {
        Some(f) => f.to_ascii_lowercase().to_string() + c.as_str(),
        None => p,
    }
}

fn arg_str(ctx: &ResolverContext<'_>, name: &str) -> async_graphql::Result<Option<String>> {
    match ctx.args.get(name) {
        Some(v) if !v.is_null() => Ok(Some(v.string()?.to_string())),
        _ => Ok(None),
    }
}

fn arg_i64(ctx: &ResolverContext<'_>, name: &str) -> async_graphql::Result<Option<i64>> {
    match ctx.args.get(name) {
        Some(v) if !v.is_null() => Ok(Some(v.i64()?)),
        _ => Ok(None),
    }
}

fn arg_bool(ctx: &ResolverContext<'_>, name: &str) -> async_graphql::Result<bool> {
    match ctx.args.get(name) {
        Some(v) if !v.is_null() => Ok(v.boolean()?),
        _ => Ok(false),
    }
}

fn arg_json(ctx: &ResolverContext<'_>, name: &str) -> async_graphql::Result<Option<Value>> {
    match ctx.args.get(name) {
        Some(v) if !v.is_null() => Ok(Some(v.as_value().clone().into_json()?)),
        _ => Ok(None),
    }
}

fn arg_strings(ctx: &ResolverContext<'_>, name: &str) -> async_graphql::Result<Vec<String>> {
    match ctx.args.get(name) {
        Some(v) if !v.is_null() => v.list()?.iter().map(|x| Ok(x.string()?.to_string())).collect(),
        _ => Ok(vec![]),
    }
}

fn req_str(ctx: &ResolverContext<'_>, name: &str) -> async_graphql::Result<String> {
    arg_str(ctx, name)?.ok_or_else(|| async_graphql::Error::new(format!("{name} is required")))
}

fn json_field<F>(name: &str, f: F) -> Field
where
    F: Fn(&Rendered) -> Value + Send + Sync + 'static,
{
    Field::new(name, TypeRef::named("JSON"), move |ctx| {
        let v = ctx.parent_value.try_downcast_ref::<Rendered>().map(|r| f(r));
        FieldFuture::new(async move { Ok(Some(FieldValue::value(to_gql(v?)))) })
    })
}

fn rendered_object(name: &str) -> Object {
    let s = |n: &str, f: fn(&Rendered) -> Option<String>| {
        Field::new(n, TypeRef::named(TypeRef::STRING), move |ctx| {
            let v = ctx.parent_value.try_downcast_ref::<Rendered>().map(f);
            FieldFuture::new(async move { Ok(v?.map(|x| FieldValue::value(GqlValue::String(x)))) })
        })
    };
    Object::new(name)
        .field(s("boundedContext", |r| Some(r.bounded_context.clone())))
        .field(s("type", |r| Some(r.type_name.clone())))
        .field(s("commandId", |r| r.command_id.clone()))
        .field(s("correlationId", |r| r.metadata.as_ref().map(|m| m.correlation_id.clone())))
        .field(s("causationId", |r| r.metadata.as_ref().and_then(|m| m.causation_id.clone())))
        .field(s("clientId", |r| r.metadata.as_ref().map(|m| m.client_id.clone())))
        .field(s("createdAt", |r| r.metadata.as_ref().map(|m| m.created_at.to_rfc3339())))
        .field(Field::new("sequence", TypeRef::named(TypeRef::INT), |ctx| {
            let v = ctx.parent_value.try_downcast_ref::<Rendered>().map(|r| r.sequence);
            FieldFuture::new(async move { Ok(v?.map(|x| FieldValue::value(GqlValue::from(x)))) })
        }))
        .field(json_field("payload", |r| r.payload.clone()))
        .field(json_field("tags", |r| serde_json::to_value(&r.tags).unwrap_or_default()))
        .field(json_field("origin", |r| serde_json::to_value(&r.origin).unwrap_or_default()))
        .field(json_field("metadata", |r| serde_json::to_value(&r.metadata).unwrap_or_default()))
}

/// A command outcome as the GraphQL caller sees it.
#[derive(Clone)]
struct OutcomeView(CommandOutcome, Vec<Rendered>);

fn outcome_object() -> Object {
    fn get<'a>(ctx: &'a ResolverContext<'_>) -> async_graphql::Result<&'a OutcomeView> {
        ctx.parent_value.try_downcast_ref::<OutcomeView>()
    }
    let st = |n: &'static str, f: fn(&CommandOutcome) -> Option<String>| {
        Field::new(n, TypeRef::named(TypeRef::STRING), move |ctx| {
            let v = get(&ctx).map(|o| f(&o.0));
            FieldFuture::new(async move { Ok(v?.map(|x| FieldValue::value(GqlValue::String(x)))) })
        })
    };
    Object::new("CommandResult")
        .field(st("status", |o| {
            Some(
                match o {
                    CommandOutcome::Accepted { .. } => "ACCEPTED",
                    CommandOutcome::Duplicate { .. } => "DUPLICATE",
                    CommandOutcome::Rejected { .. } => "REJECTED",
                    CommandOutcome::Refused { .. } => "REFUSED",
                    CommandOutcome::NotProcessed => "NOT_PROCESSED",
                }
                .to_string(),
            )
        }))
        .field(st("commandId", |o| match o {
            CommandOutcome::Accepted { command, .. } => Some(command.id.clone()),
            CommandOutcome::Duplicate { command_id, .. } => Some(command_id.clone()),
            _ => None,
        }))
        .field(st("rejectionReason", |o| match o {
            CommandOutcome::Rejected { reason, .. } => Some(reason.clone()),
            _ => None,
        }))
        .field(st("rejectionKind", |o| match o {
            CommandOutcome::Rejected { kind, .. } => Some(kind.clone()),
            _ => None,
        }))
        .field(st("errorCode", |o| match o {
            CommandOutcome::Refused { code, .. } => Some(code.to_string()),
            _ => None,
        }))
        .field(st("errorMessage", |o| match o {
            CommandOutcome::Refused { message, .. } => Some(message.clone()),
            _ => None,
        }))
        .field(Field::new("highestSequence", TypeRef::named(TypeRef::INT), |ctx| {
            let v = get(&ctx).map(|o| o.0.events().iter().map(|e| e.sequence).max());
            FieldFuture::new(async move { Ok(v?.map(|x| FieldValue::value(GqlValue::from(x)))) })
        }))
        .field(Field::new("events", TypeRef::named_nn_list_nn("Event"), |ctx| {
            let v = get(&ctx).map(|o| o.1.clone());
            FieldFuture::new(async move { Ok(Some(FieldValue::list(v?.into_iter().map(FieldValue::owned_any)))) })
        }))
        .field(Field::new("matchingEvents", TypeRef::named_nn_list("Event"), |ctx| {
            let v = get(&ctx).map(|o| match &o.0 {
                CommandOutcome::Rejected { matching_events, .. } => matching_events.clone(),
                _ => None,
            });
            FieldFuture::new(async move { Ok(v?.map(|m| FieldValue::list(m.into_iter().map(FieldValue::owned_any)))) })
        }))
        .field(Field::new("matchingEventsTruncated", TypeRef::named(TypeRef::BOOLEAN), |ctx| {
            let v = get(&ctx).map(|o| matches!(&o.0, CommandOutcome::Rejected { truncated: true, .. }));
            FieldFuture::new(async move { Ok(Some(FieldValue::value(GqlValue::Boolean(v?)))) })
        }))
}

/// Render an outcome's events for the caller.
async fn outcome_view(s: &Skilj, g: &Grant, o: CommandOutcome) -> async_graphql::Result<OutcomeView> {
    let mut rendered = vec![];
    for e in o.events() {
        rendered.push(s.render_for(g, e).await.map_err(gql_err)?);
    }
    Ok(OutcomeView(o, rendered))
}

// ------------------------------------------------- schema from shapes

struct TypeGen {
    objects: Vec<Object>,
    inputs: Vec<InputObject>,
    names: BTreeSet<String>,
}

impl TypeGen {
    fn unique(&mut self, name: String) -> String {
        let mut n = name.clone();
        let mut i = 2;
        while self.names.contains(&n) {
            n = format!("{name}{i}");
            i += 1;
        }
        self.names.insert(n.clone());
        n
    }

    /// Output type for a shape (values come as owned serde_json::Value).
    fn output(&mut self, prefix: &str, shape: &Shape, required: bool) -> TypeRef {
        let base = match shape {
            Shape::Scalar(s) => TypeRef::named(scalar_name(s.ty)),
            Shape::Array(inner) => {
                let i = self.output(prefix, inner, true);
                return if required { TypeRef::NonNull(Box::new(TypeRef::List(Box::new(i)))) } else { TypeRef::List(Box::new(i)) };
            }
            Shape::Object(o) if o.fields.is_empty() => TypeRef::named("JSON"),
            Shape::Object(o) => {
                let name = self.unique(format!("{prefix}{}", o.name.as_deref().map(pascal).unwrap_or_default()));
                let mut obj = Object::new(&name);
                for (fname, f) in &o.fields {
                    let gname = graphql_field_name(fname);
                    let ty = self.output(&format!("{name}{}", pascal(fname)), &f.shape, false);
                    let key = fname.clone();
                    let shape = f.shape.clone();
                    obj = obj.field(Field::new(gname, ty, move |ctx| {
                        let v = ctx
                            .parent_value
                            .try_downcast_ref::<Value>()
                            .map(|p| p.get(&key).cloned().unwrap_or(Value::Null));
                        let shape = shape.clone();
                        FieldFuture::new(async move { Ok(json_to_field(v?, &shape)) })
                    }));
                }
                self.objects.push(obj);
                TypeRef::named(name)
            }
        };
        if required {
            TypeRef::NonNull(Box::new(base))
        } else {
            base
        }
    }

    /// Input type for a command payload shape.
    fn input(&mut self, prefix: &str, shape: &Shape, required: bool) -> TypeRef {
        let base = match shape {
            Shape::Scalar(s) => TypeRef::named(scalar_name(s.ty)),
            Shape::Array(inner) => {
                let i = self.input(prefix, inner, true);
                return if required { TypeRef::NonNull(Box::new(TypeRef::List(Box::new(i)))) } else { TypeRef::List(Box::new(i)) };
            }
            Shape::Object(o) if o.fields.is_empty() => TypeRef::named("JSON"),
            Shape::Object(o) => {
                let name = self.unique(format!("{prefix}Input"));
                let mut obj = InputObject::new(&name);
                for (fname, f) in &o.fields {
                    let ty = self.input(&format!("{prefix}{}", pascal(fname)), &f.shape, f.required);
                    obj = obj.field(InputValue::new(graphql_field_name(fname), ty));
                }
                self.inputs.push(obj);
                TypeRef::named(name)
            }
        };
        if required {
            TypeRef::NonNull(Box::new(base))
        } else {
            base
        }
    }
}

fn graphql_field_name(n: &str) -> String {
    // Payload field names are JSON keys; GraphQL needs identifiers. We keep
    // them as they are when they already are one.
    if n.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_') && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        n.to_string()
    } else {
        format!("_{}", n.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '_' }).collect::<String>())
    }
}

fn scalar_name(t: skilj_core::schema::ScalarType) -> &'static str {
    use skilj_core::schema::ScalarType::*;
    match t {
        String => TypeRef::STRING,
        Integer => TypeRef::INT,
        Number => TypeRef::FLOAT,
        Boolean => TypeRef::BOOLEAN,
    }
}

/// A JSON value as a field value of the given shape; a value that doesn't
/// fit (a ciphertext string in an integer field) becomes null.
fn json_to_field(v: Value, shape: &Shape) -> Option<FieldValue<'static>> {
    if v.is_null() {
        return None;
    }
    match shape {
        Shape::Scalar(s) => {
            if s.ty.accepts(&v) {
                Some(FieldValue::value(to_gql(v)))
            } else {
                None
            }
        }
        Shape::Array(inner) => match v {
            Value::Array(items) => Some(FieldValue::list(items.into_iter().filter_map(|i| json_to_field(i, inner)))),
            _ => None,
        },
        Shape::Object(o) if o.fields.is_empty() => Some(FieldValue::value(to_gql(v))),
        Shape::Object(_) => v.is_object().then(|| FieldValue::owned_any(v)),
    }
}

/// A fingerprint of everything the schema is generated from.
pub fn fingerprint(reg: &Registry) -> u64 {
    let mut h = DefaultHasher::new();
    for (n, c) in &reg.contexts {
        n.hash(&mut h);
        format!("{:?}", c.status).hash(&mut h);
    }
    for ((b, n), t) in &reg.event_types {
        (b, n, t.0.schema_version).hash(&mut h);
    }
    for ((b, n), t) in &reg.command_types {
        (b, n, t.0.schema_version).hash(&mut h);
    }
    for ((b, n), p) in &reg.projections {
        (b, n, p.schema_version).hash(&mut h);
    }
    h.finish()
}

/// Build the schema for the current registry.
pub fn build(reg: &Registry) -> Result<Schema, SchemaError> {
    let mut gen = TypeGen {
        objects: vec![],
        inputs: vec![],
        names: ["Query", "Mutation", "Subscription", "Event", "CommandResult", "JSON", "FilterInput", "CommandInput"]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    };
    let mut query = Object::new("Query");
    let mut mutation = Object::new("Mutation");
    let mut subscription = Subscription::new("Subscription");

    query = global_queries(query);
    mutation = global_mutations(mutation);

    for name in reg.contexts.keys() {
        let ns = pascal(name);
        let qname = gen.unique(format!("{ns}Query"));
        let mut q = Object::new(&qname);
        // Projections, typed.
        for p in reg.projections_of(name) {
            let Ok(schema) = PSchema::parse(&p.schema) else { continue };
            let tname = format!("{ns}{}", pascal(&p.name));
            let ty = gen.output(&tname, &Shape::Object(schema.root.clone()), false);
            let pname = p.name.clone();
            q = q.field(
                Field::new(graphql_field_name(&p.name), ty, move |ctx| {
                    let pname = pname.clone();
                    FieldFuture::new(async move {
                        let g = ctx.parent_value.try_downcast_ref::<Grant>()?.clone();
                        let s = ctx_of(&ctx)?.skilj.clone();
                        let key = arg_str(&ctx, "key")?;
                        let wait = arg_i64(&ctx, "waitForSequence")?;
                        let v = s.query_projection(&g, &pname, key.as_deref(), wait).await.map_err(gql_err)?;
                        Ok(v.map(FieldValue::owned_any))
                    })
                })
                .argument(InputValue::new("key", TypeRef::named(TypeRef::STRING)))
                .argument(InputValue::new("waitForSequence", TypeRef::named(TypeRef::INT))),
            );
        }
        q = context_queries(q);
        gen.objects.push(q);
        let bc = name.clone();
        query = query.field(Field::new(name.clone(), TypeRef::named_nn(&qname), move |ctx| {
            let bc = bc.clone();
            FieldFuture::new(async move {
                let g = grant(&ctx, &bc).await?;
                Ok(Some(FieldValue::owned_any(g)))
            })
        }));

        // Typed command mutations.
        for t in reg.command_types_of(name) {
            let tname = format!("{ns}{}", pascal(&t.0.name));
            let input = gen.input(&tname, &Shape::Object(t.1.root.clone()), true);
            let bc = name.clone();
            let ct = t.0.name.clone();
            mutation = mutation.field(
                command_args(Field::new(format!("{name}_{}", lower_camel(&t.0.name)), TypeRef::named_nn("CommandResult"), move |ctx| {
                    let bc = bc.clone();
                    let ct = ct.clone();
                    FieldFuture::new(async move {
                        let g = grant(&ctx, &bc).await?;
                        let payload = arg_json(&ctx, "input")?.unwrap_or(Value::Null);
                        let req = command_request(&ctx, ct, payload)?;
                        let s = ctx_of(&ctx)?.skilj.clone();
                        let o = s.submit_command(&g, req).await.map_err(gql_err)?;
                        Ok(Some(FieldValue::owned_any(outcome_view(&s, &g, o).await?)))
                    })
                }))
                .argument(InputValue::new("input", input)),
            );
        }
        mutation = context_mutations(mutation, name);

        // Subscriptions.
        subscription = subscription.field(subscription_field(format!("{name}_events"), name.clone(), None));
        for t in reg.event_types_of(name) {
            subscription = subscription.field(subscription_field(
                format!("{name}_{}", graphql_field_name(&t.0.name)),
                name.clone(),
                Some(t.0.name.clone()),
            ));
        }
    }

    let mut b = Schema::build("Query", Some("Mutation"), Some("Subscription"))
        .register(Scalar::new("JSON"))
        .register(rendered_object("Event"))
        .register(outcome_object())
        .register(
            InputObject::new("FilterInput")
                .field(InputValue::new("field", TypeRef::named_nn(TypeRef::STRING)))
                .field(InputValue::new("operator", TypeRef::named_nn(TypeRef::STRING)))
                .field(InputValue::new("value", TypeRef::named_nn(TypeRef::STRING))),
        )
        .register(
            InputObject::new("CommandInput")
                .field(InputValue::new("type", TypeRef::named_nn(TypeRef::STRING)))
                .field(InputValue::new("payload", TypeRef::named_nn("JSON")))
                .field(InputValue::new("idempotencyKey", TypeRef::named(TypeRef::STRING)))
                .field(InputValue::new("correlationId", TypeRef::named(TypeRef::STRING)))
                .field(InputValue::new("causationId", TypeRef::named(TypeRef::STRING))),
        )
        .register(query)
        .register(mutation)
        .register(subscription)
        .limit_depth(16)
        .limit_complexity(5000);
    for o in gen.objects {
        b = b.register(o);
    }
    for i in gen.inputs {
        b = b.register(i);
    }
    b.finish()
}

fn command_args(f: Field) -> Field {
    f.argument(InputValue::new("idempotencyKey", TypeRef::named(TypeRef::STRING)))
        .argument(InputValue::new("correlationId", TypeRef::named(TypeRef::STRING)))
        .argument(InputValue::new("causationId", TypeRef::named(TypeRef::STRING)))
}

fn command_request(ctx: &ResolverContext<'_>, ct: String, payload: Value) -> async_graphql::Result<CommandRequest> {
    Ok(CommandRequest {
        command_type: ct,
        payload,
        idempotency_key: arg_str(ctx, "idempotencyKey")?,
        correlation_id: arg_str(ctx, "correlationId")?,
        causation_id: arg_str(ctx, "causationId")?,
    })
}

fn parse_filters(ctx: &ResolverContext<'_>) -> async_graphql::Result<Vec<Filter>> {
    match ctx.args.get("filters") {
        Some(v) if !v.is_null() => v
            .list()?
            .iter()
            .map(|f| {
                let o = f.object()?;
                let op = o.try_get("operator")?.string()?.to_lowercase();
                Ok(Filter {
                    field: o.try_get("field")?.string()?.to_string(),
                    operator: serde_json::from_value(Value::String(op.clone()))
                        .map_err(|_| gql_err(Error::invalid(format!("unknown operator {op}"))))?,
                    value: o.try_get("value")?.string()?.to_string(),
                })
            })
            .collect(),
        _ => Ok(vec![]),
    }
}

fn subscription_field(name: String, bc: String, event_type: Option<String>) -> SubscriptionField {
    let f = SubscriptionField::new(name, TypeRef::named_nn("Event"), move |ctx| {
        let bc = bc.clone();
        let et = event_type.clone();
        SubscriptionFieldFuture::new(async move {
            let g = grant(&ctx, &bc).await?;
            let c = ctx_of(&ctx)?.clone();
            let expires = c.identity.as_ref().map(|i| i.expires_at).unwrap_or(0);
            let types = arg_strings(&ctx, "eventTypes")?;
            let filters = parse_filters(&ctx)?;
            let from = arg_i64(&ctx, "fromSequence")?;
            let epoch = arg_str(&ctx, "epoch")?;
            let (spec, resume) = c
                .skilj
                .create_subscription(&g, et.as_deref(), &types, &filters, from, epoch.as_deref())
                .await
                .map_err(gql_err)?;
            let live = c.skilj.subscription_stream(g, spec, resume).map(|item| match item {
                Ok(r) => Ok(FieldValue::owned_any(r)),
                Err(SubscriptionEnd::Revoked) => Err(async_graphql::Error::new("subscription revoked: the grant behind it no longer exists")
                    .extend_with(|_, e| e.set("code", "REVOKED"))),
                Err(SubscriptionEnd::Lagged) => Err(async_graphql::Error::new("subscription ended: reconnect from the last sequence received")
                    .extend_with(|_, e| e.set("code", "RECONNECT"))),
            });
            // The connection lasts no longer than the JWT.
            let until = std::time::Duration::from_secs((expires - chrono::Utc::now().timestamp()).max(0) as u64);
            let stream = futures::stream::unfold((live, false), |(mut live, done)| async move {
                if done {
                    return None;
                }
                let item = live.next().await?;
                let end = item.is_err();
                Some((item, (live, end)))
            });
            Ok(stream.take_until(tokio::time::sleep(until)))
        })
    })
    .argument(InputValue::new("fromSequence", TypeRef::named(TypeRef::INT)))
    .argument(InputValue::new("epoch", TypeRef::named(TypeRef::STRING)));
    let f = f.argument(InputValue::new("eventTypes", TypeRef::named_nn_list(TypeRef::STRING)))
        .argument(InputValue::new("filters", TypeRef::named_nn_list("FilterInput")));
    f
}

/// Fields every context's query object has (parent value: the Grant).
fn context_queries(q: Object) -> Object {
    fn g<'a>(ctx: &'a ResolverContext<'_>) -> async_graphql::Result<&'a Grant> {
        ctx.parent_value.try_downcast_ref::<Grant>()
    }
    q.field(
        Field::new("events", TypeRef::named_nn("JSON"), |ctx| {
            FieldFuture::new(async move {
                let grant = g(&ctx)?.clone();
                let s = ctx_of(&ctx)?.skilj.clone();
                let types = arg_strings(&ctx, "eventTypes")?;
                let tags: Option<Vec<Tag>> = arg_json(&ctx, "tags")?.map(serde_json::from_value).transpose()?;
                let after = arg_i64(&ctx, "afterSequence")?;
                let page = s.query_events(&grant, &types, tags, after).await.map_err(gql_err)?;
                Ok(Some(FieldValue::value(to_gql(json!({"events": page.events, "epoch": page.epoch, "lastSequence": page.last_sequence})))))
            })
        })
        .argument(InputValue::new("eventTypes", TypeRef::named_nn_list(TypeRef::STRING)))
        .argument(InputValue::new("tags", TypeRef::named("JSON")))
        .argument(InputValue::new("afterSequence", TypeRef::named(TypeRef::INT))),
    )
    .field(
        Field::new("countEvents", TypeRef::named_nn(TypeRef::INT), |ctx| {
            FieldFuture::new(async move {
                let grant = g(&ctx)?.clone();
                let s = ctx_of(&ctx)?.skilj.clone();
                let types = arg_strings(&ctx, "eventTypes")?;
                let tags: Option<Vec<Tag>> = arg_json(&ctx, "tags")?.map(serde_json::from_value).transpose()?;
                let n = s.count_events(&grant, &types, tags).await.map_err(gql_err)?;
                Ok(Some(FieldValue::value(GqlValue::from(n as i64))))
            })
        })
        .argument(InputValue::new("eventTypes", TypeRef::named_nn_list(TypeRef::STRING)))
        .argument(InputValue::new("tags", TypeRef::named("JSON"))),
    )
    .field(
        Field::new("event", TypeRef::named("Event"), |ctx| {
            FieldFuture::new(async move {
                let grant = g(&ctx)?.clone();
                let s = ctx_of(&ctx)?.skilj.clone();
                let seq = arg_i64(&ctx, "sequence")?.unwrap_or(-1);
                let r = s.inspect_event(&grant, seq).await.map_err(gql_err)?;
                Ok(Some(FieldValue::owned_any(r)))
            })
        })
        .argument(InputValue::new("sequence", TypeRef::named_nn(TypeRef::INT))),
    )
    .field(
        Field::new("commands", TypeRef::named_nn_list_nn("Event"), |ctx| {
            FieldFuture::new(async move {
                let grant = g(&ctx)?.clone();
                let s = ctx_of(&ctx)?.skilj.clone();
                let types = arg_strings(&ctx, "commandTypes")?;
                let parse = |n: &str| -> async_graphql::Result<Option<chrono::DateTime<chrono::Utc>>> {
                    Ok(arg_str(&ctx, n)?
                        .map(|t| chrono::DateTime::parse_from_rfc3339(&t).map(|d| d.with_timezone(&chrono::Utc)))
                        .transpose()?)
                };
                let after = parse("after")?;
                let before = parse("before")?;
                let triggered = arg_i64(&ctx, "triggeredEvent")?;
                let after_command = arg_str(&ctx, "afterCommand")?;
                let out = s
                    .fetch_commands(&grant, &types, after, before, triggered, after_command.as_deref())
                    .await
                    .map_err(gql_err)?;
                Ok(Some(FieldValue::list(out.into_iter().map(FieldValue::owned_any))))
            })
        })
        .argument(InputValue::new("commandTypes", TypeRef::named_nn_list(TypeRef::STRING)))
        .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
        .argument(InputValue::new("before", TypeRef::named(TypeRef::STRING)))
        .argument(InputValue::new("triggeredEvent", TypeRef::named(TypeRef::INT)))
        .argument(InputValue::new("afterCommand", TypeRef::named(TypeRef::STRING))),
    )
    .field(
        Field::new("snapshot", TypeRef::named("JSON"), |ctx| {
            FieldFuture::new(async move {
                let grant = g(&ctx)?.clone();
                let s = ctx_of(&ctx)?.skilj.clone();
                let v = s
                    .inspect_snapshot(&grant, &req_str(&ctx, "name")?, &req_str(&ctx, "tagValue")?)
                    .await
                    .map_err(gql_err)?;
                Ok(v.map(|v| FieldValue::value(to_gql(v))))
            })
        })
        .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
        .argument(InputValue::new("tagValue", TypeRef::named_nn(TypeRef::STRING))),
    )
    .field(
        Field::new("privateFieldGrants", TypeRef::named_nn("JSON"), |ctx| {
            FieldFuture::new(async move {
                let grant = g(&ctx)?.clone();
                let s = ctx_of(&ctx)?.skilj.clone();
                let grantor = arg_str(&ctx, "grantor")?;
                let v = s.list_private_field_grants(&grant, grantor.as_deref()).await.map_err(gql_err)?;
                Ok(Some(FieldValue::value(ser(&v))))
            })
        })
        .argument(InputValue::new("grantor", TypeRef::named(TypeRef::STRING))),
    )
    .field(Field::new("grant", TypeRef::named_nn("JSON"), |ctx| {
        FieldFuture::new(async move {
            let grant = g(&ctx)?.clone();
            Ok(Some(FieldValue::value(ser(&grant.mapping))))
        })
    }))
    .field(Field::new("registration", TypeRef::named_nn("JSON"), |ctx| {
        FieldFuture::new(async move {
            let grant = g(&ctx)?.clone();
            let s = ctx_of(&ctx)?.skilj.clone();
            let v = s.type_registration_view(&grant).await.map_err(gql_err)?;
            Ok(Some(FieldValue::value(to_gql(v))))
        })
    }))
    .field(Field::new("tokens", TypeRef::named_nn("JSON"), |ctx| {
        FieldFuture::new(async move {
            let grant = g(&ctx)?.clone();
            let s = ctx_of(&ctx)?.skilj.clone();
            let v = s.list_tokens(&grant).await.map_err(gql_err)?;
            Ok(Some(FieldValue::value(ser(&v))))
        })
    }))
    .field(Field::new("encryptionKeys", TypeRef::named_nn("JSON"), |ctx| {
        FieldFuture::new(async move {
            let grant = g(&ctx)?.clone();
            let s = ctx_of(&ctx)?.skilj.clone();
            let v = s.list_encryption_keys(&grant).await.map_err(gql_err)?;
            Ok(Some(FieldValue::value(ser(&v))))
        })
    }))
    .field(Field::new("delegation", TypeRef::named_nn("JSON"), |ctx| {
        FieldFuture::new(async move {
            let grant = g(&ctx)?.clone();
            let s = ctx_of(&ctx)?.skilj.clone();
            let v = s.delegation_view(&grant).await.map_err(gql_err)?;
            let rows: Vec<Value> = v
                .into_iter()
                .map(|(id, name, m)| json!({"roleId": id, "roleName": name, "mappingId": m.id, "capabilities": m.capabilities}))
                .collect();
            Ok(Some(FieldValue::value(to_gql(json!({"delegable": grant.mapping.delegable, "grants": rows})))))
        })
    }))
}

/// Per-context mutations, prefixed with the context name.
fn context_mutations(mut m: Object, bc: &str) -> Object {
    type Op = fn(Skilj, Grant, ResolverContext<'_>) -> futures::future::BoxFuture<'_, async_graphql::Result<GqlValue>>;
    let add = |m: Object, name: &str, args: Vec<InputValue>, op: Op| -> Object {
        let bcn = bc.to_string();
        let mut f = Field::new(format!("{bc}_{name}"), TypeRef::named("JSON"), move |ctx| {
            let bcn = bcn.clone();
            FieldFuture::new(async move {
                let g = grant(&ctx, &bcn).await?;
                let s = ctx_of(&ctx)?.skilj.clone();
                let v = op(s, g, ctx).await?;
                Ok(Some(FieldValue::value(v)))
            })
        });
        for a in args {
            f = f.argument(a);
        }
        m.field(f)
    };
    let s_ = |n: &str| InputValue::new(n, TypeRef::named(TypeRef::STRING));
    let sn = |n: &str| InputValue::new(n, TypeRef::named_nn(TypeRef::STRING));
    let i_ = |n: &str| InputValue::new(n, TypeRef::named(TypeRef::INT));

    // Generic submission and batch.
    let bcn = bc.to_string();
    m = m.field(
        command_args(Field::new(format!("{bc}_submit"), TypeRef::named_nn("CommandResult"), move |ctx| {
            let bcn = bcn.clone();
            FieldFuture::new(async move {
                let g = grant(&ctx, &bcn).await?;
                let s = ctx_of(&ctx)?.skilj.clone();
                let req = command_request(&ctx, req_str(&ctx, "type")?, arg_json(&ctx, "payload")?.unwrap_or(Value::Null))?;
                let o = s.submit_command(&g, req).await.map_err(gql_err)?;
                Ok(Some(FieldValue::owned_any(outcome_view(&s, &g, o).await?)))
            })
        }))
        .argument(sn("type"))
        .argument(InputValue::new("payload", TypeRef::named_nn("JSON"))),
    );
    let bcn = bc.to_string();
    m = m.field(
        Field::new(format!("{bc}_submitCommands"), TypeRef::named_nn_list_nn("CommandResult"), move |ctx| {
            let bcn = bcn.clone();
            FieldFuture::new(async move {
                let g = grant(&ctx, &bcn).await?;
                let s = ctx_of(&ctx)?.skilj.clone();
                let items = arg_json(&ctx, "commands")?.unwrap_or(json!([]));
                let reqs: Vec<CommandRequest> = items
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|c| CommandRequest {
                        command_type: c["type"].as_str().unwrap_or_default().to_string(),
                        payload: c["payload"].clone(),
                        idempotency_key: c["idempotencyKey"].as_str().map(str::to_string),
                        correlation_id: c["correlationId"].as_str().map(str::to_string),
                        causation_id: c["causationId"].as_str().map(str::to_string),
                    })
                    .collect();
                let out = s.submit_commands(&g, reqs).await.map_err(gql_err)?;
                let mut views = vec![];
                for o in out {
                    views.push(FieldValue::owned_any(outcome_view(&s, &g, o).await?));
                }
                Ok(Some(FieldValue::list(views)))
            })
        })
        .argument(InputValue::new("commands", TypeRef::named_nn_list_nn("CommandInput"))),
    );

    m = add(m, "dryRun", vec![sn("type"), InputValue::new("payload", TypeRef::named_nn("JSON"))], |s, g, ctx| {
        Box::pin(async move {
            let d = s
                .dry_run_command(&g, &req_str(&ctx, "type")?, arg_json(&ctx, "payload")?.unwrap_or(Value::Null))
                .await
                .map_err(gql_err)?;
            Ok(to_gql(json!({"accepted": d.accepted, "rejectionReason": d.rejection_reason, "rejectionKind": d.rejection_kind,
                "wouldBeEvents": d.would_be_events, "matchingEvents": d.matching_events})))
        })
    });
    m = add(
        m,
        "mintToken",
        vec![sn("purpose"), sn("type"), s_("scope"), s_("startFrom"), i_("startAtSequence"), s_("startAtTime")],
        |s, g, ctx| {
            Box::pin(async move {
                let purpose = TokenPurpose::parse(&req_str(&ctx, "purpose")?.to_lowercase())
                    .ok_or_else(|| gql_err(Error::invalid("purpose is external_event, direct_creation, command or event_read")))?;
                let start = arg_str(&ctx, "startFrom")?
                    .map(|x| EventReadStartPosition::parse(&x.to_lowercase()).ok_or_else(|| gql_err(Error::invalid("bad startFrom"))))
                    .transpose()?;
                let at_time = arg_str(&ctx, "startAtTime")?
                    .map(|t| chrono::DateTime::parse_from_rfc3339(&t).map(|d| d.with_timezone(&chrono::Utc)))
                    .transpose()?;
                let t = s
                    .mint_token(&g, purpose, &req_str(&ctx, "type")?, arg_str(&ctx, "scope")?.as_deref(), start, arg_i64(&ctx, "startAtSequence")?, at_time)
                    .await
                    .map_err(gql_err)?;
                Ok(ser(&t))
            })
        },
    );
    m = add(m, "revokeToken", vec![sn("id")], |s, g, ctx| {
        Box::pin(async move {
            s.revoke_token(&g, &req_str(&ctx, "id")?).await.map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    m = add(m, "archive", vec![], |s, g, _| {
        Box::pin(async move {
            s.archive_bounded_context(&g).await.map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    m = add(m, "forgetSubject", vec![sn("subjectKey"), sn("subjectValue")], |s, g, ctx| {
        Box::pin(async move {
            s.forget_subject(&g, &req_str(&ctx, "subjectKey")?, &req_str(&ctx, "subjectValue")?)
                .await
                .map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    m = add(m, "rebuildProjection", vec![sn("name")], |s, g, ctx| {
        Box::pin(async move {
            s.rebuild_projection(&g, &req_str(&ctx, "name")?).await.map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    m = add(m, "discardProjectionRebuild", vec![sn("name")], |s, g, ctx| {
        Box::pin(async move {
            s.discard_projection_rebuild(&g, &req_str(&ctx, "name")?).await.map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    m = add(m, "grantCapability", vec![sn("mappingId"), sn("capability")], |s, g, ctx| {
        Box::pin(async move {
            s.grant_capability_as_admin(&g, &req_str(&ctx, "mappingId")?, &req_str(&ctx, "capability")?)
                .await
                .map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    m = add(m, "revokeCapability", vec![sn("mappingId"), sn("capability")], |s, g, ctx| {
        Box::pin(async move {
            s.revoke_capability_as_admin(&g, &req_str(&ctx, "mappingId")?, &req_str(&ctx, "capability")?)
                .await
                .map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    m = add(m, "grantPrivateFieldAccess", vec![sn("granteeRoleId"), i_("eventSequence"), s_("commandId")], |s, g, ctx| {
        Box::pin(async move {
            let r = s
                .grant_private_field_access(&g, &req_str(&ctx, "granteeRoleId")?, arg_i64(&ctx, "eventSequence")?, arg_str(&ctx, "commandId")?.as_deref())
                .await
                .map_err(gql_err)?;
            Ok(ser(&r))
        })
    });
    m = add(m, "revokePrivateFieldAccess", vec![sn("grantId")], |s, g, ctx| {
        Box::pin(async move {
            s.revoke_private_field_access(&g, &req_str(&ctx, "grantId")?).await.map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    let _ = arg_bool;
    m
}

fn global_queries(q: Object) -> Object {
    q.field(Field::new("me", TypeRef::named("JSON"), |ctx| {
        FieldFuture::new(async move {
            let (s, sub) = identity(&ctx)?;
            let r = s.role_by_subject(&sub).await.map_err(gql_err)?;
            Ok(r.map(|r| FieldValue::value(ser(&json!({"id": r.id, "name": r.name, "superadmin": r.superadmin})))))
        })
    }))
    .field(Field::new("epoch", TypeRef::named_nn(TypeRef::STRING), |ctx| {
        FieldFuture::new(async move {
            let s = ctx_of(&ctx)?.skilj.clone();
            Ok(Some(FieldValue::value(GqlValue::String(s.current_epoch().await))))
        })
    }))
    .field(Field::new("boundedContexts", TypeRef::named_nn("JSON"), |ctx| {
        FieldFuture::new(async move {
            let (s, r) = superadmin(&ctx).await?;
            let v = s.list_bounded_contexts(&r).await.map_err(gql_err)?;
            let rows: Vec<Value> = v
                .into_iter()
                .map(|e| {
                    json!({
                        "name": e.context.name, "status": e.context.status, "createdAt": e.context.created_at,
                        "createdBy": e.context.created_by, "template": e.context.template,
                        "access": e.mappings.into_iter().map(|(r, m)| json!({
                            "roleId": r.id, "roleName": r.name, "externalSubject": r.external_subject,
                            "mappingId": m.id, "level": m.level, "canReadSensitive": m.can_read_sensitive,
                            "scope": m.scope, "active": m.active})).collect::<Vec<_>>()
                    })
                })
                .collect();
            Ok(Some(FieldValue::value(to_gql(Value::Array(rows)))))
        })
    }))
    .field(Field::new("roles", TypeRef::named_nn("JSON"), |ctx| {
        FieldFuture::new(async move {
            let (s, r) = superadmin(&ctx).await?;
            Ok(Some(FieldValue::value(ser(&s.list_roles(&r).await.map_err(gql_err)?))))
        })
    }))
    .field(
        Field::new("accessMappings", TypeRef::named_nn("JSON"), |ctx| {
            FieldFuture::new(async move {
                let (s, r) = superadmin(&ctx).await?;
                let bc = arg_str(&ctx, "boundedContext")?;
                Ok(Some(FieldValue::value(ser(&s.list_mappings(&r, bc.as_deref()).await.map_err(gql_err)?))))
            })
        })
        .argument(InputValue::new("boundedContext", TypeRef::named(TypeRef::STRING))),
    )
    .field(Field::new("capabilityOverview", TypeRef::named_nn("JSON"), |ctx| {
        FieldFuture::new(async move {
            let (s, r) = superadmin(&ctx).await?;
            Ok(Some(FieldValue::value(ser(&s.capability_overview(&r).await.map_err(gql_err)?))))
        })
    }))
    .field(
        Field::new("capabilityHistory", TypeRef::named_nn("JSON"), |ctx| {
            FieldFuture::new(async move {
                let (s, r) = superadmin(&ctx).await?;
                let as_of = match arg_str(&ctx, "asOf")? {
                    Some(t) => chrono::DateTime::parse_from_rfc3339(&t)?.with_timezone(&chrono::Utc),
                    None => chrono::Utc::now(),
                };
                let v = s
                    .capability_history(&r, &req_str(&ctx, "roleId")?, &req_str(&ctx, "boundedContext")?, as_of)
                    .await
                    .map_err(gql_err)?;
                Ok(Some(FieldValue::value(ser(&v))))
            })
        })
        .argument(InputValue::new("roleId", TypeRef::named_nn(TypeRef::STRING)))
        .argument(InputValue::new("boundedContext", TypeRef::named_nn(TypeRef::STRING)))
        .argument(InputValue::new("asOf", TypeRef::named(TypeRef::STRING))),
    )
}

fn global_mutations(m: Object) -> Object {
    type Op = fn(Skilj, Role, ResolverContext<'_>) -> futures::future::BoxFuture<'_, async_graphql::Result<GqlValue>>;
    fn add(m: Object, name: &str, args: Vec<InputValue>, op: Op) -> Object {
        let mut f = Field::new(name, TypeRef::named("JSON"), move |ctx| {
            FieldFuture::new(async move {
                let (s, r) = superadmin(&ctx).await?;
                Ok(Some(FieldValue::value(op(s, r, ctx).await?)))
            })
        });
        for a in args {
            f = f.argument(a);
        }
        m.field(f)
    }
    let sn = |n: &str| InputValue::new(n, TypeRef::named_nn(TypeRef::STRING));
    let s_ = |n: &str| InputValue::new(n, TypeRef::named(TypeRef::STRING));
    let bn = |n: &str| InputValue::new(n, TypeRef::named_nn(TypeRef::BOOLEAN));

    let m = m.field(
        Field::new("createSuperadmin", TypeRef::named_nn("JSON"), |ctx| {
            FieldFuture::new(async move {
                let s = ctx_of(&ctx)?.skilj.clone();
                let r = s
                    .create_superadmin(&req_str(&ctx, "bootstrapSecret")?, &req_str(&ctx, "name")?, &req_str(&ctx, "externalSubject")?)
                    .await
                    .map_err(gql_err)?;
                Ok(Some(FieldValue::value(to_gql(json!({"id": r.id, "name": r.name, "externalSubject": r.external_subject})))))
            })
        })
        .argument(sn("bootstrapSecret"))
        .argument(sn("name"))
        .argument(sn("externalSubject")),
    );
    let m = add(m, "addBoundedContext", vec![sn("name")], |s, r, ctx| {
        Box::pin(async move {
            s.add_bounded_context(&r, &req_str(&ctx, "name")?).await.map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    let m = add(m, "deleteBoundedContext", vec![sn("name")], |s, r, ctx| {
        Box::pin(async move {
            s.delete_bounded_context(&r, &req_str(&ctx, "name")?).await.map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    let m = add(m, "createRole", vec![sn("name"), sn("externalSubject"), bn("superadmin")], |s, r, ctx| {
        Box::pin(async move {
            let role = s
                .create_role(&r, &req_str(&ctx, "name")?, arg_bool(&ctx, "superadmin")?, &req_str(&ctx, "externalSubject")?)
                .await
                .map_err(gql_err)?;
            Ok(ser(&role))
        })
    });
    let m = add(m, "revokeRole", vec![sn("roleId")], |s, r, ctx| {
        Box::pin(async move {
            s.revoke_role(&r, &req_str(&ctx, "roleId")?).await.map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    let m = add(
        m,
        "grantAccess",
        vec![sn("roleId"), sn("boundedContext"), sn("level"), bn("canReadSensitive"), s_("scope")],
        |s, r, ctx| {
            Box::pin(async move {
                let level = AccessLevel::parse(&req_str(&ctx, "level")?.to_lowercase())
                    .ok_or_else(|| gql_err(Error::invalid("level is read, write or admin")))?;
                let m = s
                    .grant_access(&r, &req_str(&ctx, "roleId")?, &req_str(&ctx, "boundedContext")?, level, arg_bool(&ctx, "canReadSensitive")?, arg_str(&ctx, "scope")?.as_deref())
                    .await
                    .map_err(gql_err)?;
                Ok(ser(&m))
            })
        },
    );
    let m = add(m, "revokeAccess", vec![sn("mappingId")], |s, r, ctx| {
        Box::pin(async move {
            s.revoke_access(&r, &req_str(&ctx, "mappingId")?).await.map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    let m = add(m, "grantCapability", vec![sn("mappingId"), sn("capability")], |s, r, ctx| {
        Box::pin(async move {
            s.grant_capability_as_superadmin(&r, &req_str(&ctx, "mappingId")?, &req_str(&ctx, "capability")?)
                .await
                .map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    let m = add(m, "revokeCapability", vec![sn("mappingId"), sn("capability")], |s, r, ctx| {
        Box::pin(async move {
            s.revoke_capability_as_superadmin(&r, &req_str(&ctx, "mappingId")?, &req_str(&ctx, "capability")?)
                .await
                .map_err(gql_err)?;
            Ok(GqlValue::Boolean(true))
        })
    });
    let m = add(
        m,
        "setDelegableCapabilities",
        vec![sn("mappingId"), InputValue::new("delegable", TypeRef::named_nn_list_nn(TypeRef::STRING))],
        |s, r, ctx| {
            Box::pin(async move {
                let d = arg_strings(&ctx, "delegable")?.into_iter().collect();
                s.set_delegable(&r, &req_str(&ctx, "mappingId")?, d).await.map_err(gql_err)?;
                Ok(GqlValue::Boolean(true))
            })
        },
    );
    let m = add(
        m,
        "createBoundedContextFromTemplate",
        vec![sn("template"), sn("name"), sn("roleId"), sn("level"), bn("canReadSensitive"), s_("scope")],
        |s, r, ctx| {
            Box::pin(async move {
                let level = AccessLevel::parse(&req_str(&ctx, "level")?.to_lowercase())
                    .ok_or_else(|| gql_err(Error::invalid("level is read, write or admin")))?;
                let errors = s
                    .create_from_template(
                        &r,
                        &req_str(&ctx, "template")?,
                        &req_str(&ctx, "name")?,
                        &req_str(&ctx, "roleId")?,
                        level,
                        arg_bool(&ctx, "canReadSensitive")?,
                        arg_str(&ctx, "scope")?.as_deref(),
                    )
                    .await
                    .map_err(gql_err)?;
                Ok(to_gql(json!({"registrationErrors": errors})))
            })
        },
    );
    add(m, "resyncBoundedContextFromTemplate", vec![sn("name")], |s, r, ctx| {
        Box::pin(async move {
            let errors = s.resync_from_template(&r, &req_str(&ctx, "name")?).await.map_err(gql_err)?;
            Ok(to_gql(json!({"registrationErrors": errors})))
        })
    })
}

/// The schema cache: rebuilt when registered state changes, on any
/// instance (the registry mirror follows the database).
#[derive(Clone, Default)]
pub struct SchemaCache {
    inner: Arc<RwLock<Option<(u64, Schema)>>>,
}

impl SchemaCache {
    pub async fn get(&self, skilj: &Skilj) -> Result<Schema, Error> {
        let reg = skilj.registry().await;
        let fp = fingerprint(&reg);
        if let Some((f, s)) = self.inner.read().await.as_ref() {
            if *f == fp {
                return Ok(s.clone());
            }
        }
        let schema = build(&reg).map_err(|e| Error::Config(format!("schema: {e}")))?;
        *self.inner.write().await = Some((fp, schema.clone()));
        Ok(schema)
    }
}

