//! Builds the one unified GraphQL schema - see docs/architecture.md
//! §5.1. [`SchemaRegistry`] is the live, rebuildable holder of it:
//! `skilj::SkiljBuilder::build()` builds one up front and every
//! `Skilj::graphql_router()` call shares it, and
//! `skilj::cross_instance`'s dispatch loop calls
//! [`SchemaRegistry::rebuild`] whenever a `skilj_registration_changed`
//! `NOTIFY` arrives - from this process's own registration mutations
//! (every one of which also `NOTIFY`s itself, see `db::
//! notify_registration_changed`'s own doc comment) or another instance's.
//! The two cases are indistinguishable on purpose and handled
//! identically: a projection registered anywhere becomes queryable
//! everywhere, without a restart, matching `@guarantee
//! RegistrationReachesEveryInstance` in specs/skilj.allium. This also
//! fixes the same-process version of the identical gap this module used
//! to carry as a known limitation ("a projection registered after
//! `graphql_router()` was called won't gain a `ProjectionResult` member
//! until the process restarts") - multi-instance deployment was simply
//! the trigger for finally building it.

use crate::gql_types;
use crate::naming::Naming;
use crate::projection_types;
use crate::resolvers;
use crate::GraphqlState;
use arc_swap::ArcSwap;
use async_graphql::dynamic::{Field, Object, Schema, Subscription, Type};
use skilj_core::access_control::Role;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Most per-caller schemas kept at once (one per distinct set of
/// accessible bounded contexts); past it the cache starts over.
const MAX_SCOPED_SCHEMAS: usize = 1024;

/// Per-caller schemas by the bounded contexts they include, each with
/// the generation it was built under.
type ScopedSchemas = HashMap<BTreeSet<String>, (u64, Arc<Schema>)>;

/// Holds the live schema behind an `ArcSwap`, so a request in flight
/// keeps using whichever `Arc<Schema>` it already loaded even if
/// [`rebuild`](SchemaRegistry::rebuild) swaps in a new one concurrently -
/// no lock a request has to wait on, no torn reads.
///
/// The live schema holds every bounded context's projection types, named
/// after the bounded context. Only a superadmin is served it: anyone else
/// gets a schema built from just the bounded contexts they have access
/// to - none, for a caller with no credential - so neither introspection
/// nor a query's validation errors reveal which other bounded contexts
/// (tenants) exist (docs/architecture.md §138). Those are built on
/// demand and cached per set of bounded contexts until the next rebuild.
pub struct SchemaRegistry {
    current: ArcSwap<Schema>,
    /// Bumped by every [`rebuild`](SchemaRegistry::rebuild); a scoped
    /// schema built under an older one is stale.
    generation: AtomicU64,
    scoped: std::sync::Mutex<ScopedSchemas>,
    /// The published description (docs/architecture.md §194), with the
    /// generation it was built under.
    published: std::sync::Mutex<Option<(u64, Arc<PublishedDescription>)>>,
    /// Held while the published description is built, so concurrent
    /// requests after a rebuild wait for one build instead of each
    /// making their own.
    publishing: tokio::sync::Mutex<()>,
}

/// What a router composes when `/graphql` is a federation subgraph
/// (docs/architecture.md §194), and the one-field schema that answers
/// `_service` with it.
pub struct PublishedDescription {
    pub sdl: Arc<String>,
    pub service: Schema,
}

impl SchemaRegistry {
    /// Builds the initial schema - `SkiljBuilder::build()`'s own call
    /// site (`skilj/src/lib.rs`), the same "one shared, process-wide
    /// thing, constructed once in `.build()`" register
    /// `event_broadcaster`/`revocation_broadcaster`/`event_cache`
    /// already live in.
    pub async fn build(state: GraphqlState) -> skilj_core::error::Result<Self> {
        let schema = build(state, None).await?;
        Ok(Self {
            current: ArcSwap::from_pointee(schema),
            generation: AtomicU64::new(0),
            scoped: Default::default(),
            published: Default::default(),
            publishing: Default::default(),
        })
    }

    /// The live schema, with every bounded context's projection types -
    /// what a superadmin is served. Requests go through
    /// [`for_caller`](SchemaRegistry::for_caller).
    pub fn current(&self) -> Arc<Schema> {
        self.current.load_full()
    }

    /// The schema `caller` is served: the full one for a superadmin,
    /// otherwise one with only the projection types of bounded contexts
    /// `caller` has an active mapping to (docs/architecture.md §138).
    pub async fn for_caller(
        &self,
        state: &GraphqlState,
        caller: Option<&Role>,
    ) -> skilj_core::error::Result<Arc<Schema>> {
        let visible = match caller {
            Some(role) if role.superadmin => return Ok(self.current()),
            Some(role) => {
                skilj_core::db::list_accessible_bounded_context_names(&state.pool, &role.id).await?
            }
            None => BTreeSet::new(),
        };
        self.scoped(state, visible).await
    }

    /// The schema with only `visible`'s projection types, cached per set
    /// until the next [`rebuild`](SchemaRegistry::rebuild).
    async fn scoped(
        &self,
        state: &GraphqlState,
        visible: BTreeSet<String>,
    ) -> skilj_core::error::Result<Arc<Schema>> {
        let generation = self.generation.load(Ordering::Acquire);
        if let Some((built_at, schema)) = self.scoped.lock().unwrap().get(&visible) {
            if *built_at == generation {
                return Ok(schema.clone());
            }
        }
        let schema = Arc::new(build(state.clone(), Some(&visible)).await?);
        let mut scoped = self.scoped.lock().unwrap();
        // Built from what the database held before a concurrent rebuild
        // perhaps changed it: served to this caller, but not kept.
        if self.generation.load(Ordering::Acquire) == generation {
            if scoped.len() >= MAX_SCOPED_SCHEMAS && !scoped.contains_key(&visible) {
                scoped.clear();
            }
            scoped.insert(visible, (generation, schema.clone()));
        }
        Ok(schema)
    }

    /// What a router composes when `/graphql` is a federation subgraph
    /// (docs/architecture.md §194): the schema for the bounded contexts
    /// `state.federation` publishes, leaving out any made from a template
    /// (`@guarantee TenantsAreNeverPublished`) or no longer there, as
    /// federation SDL. The same for every caller, credential or not.
    /// `None` when `/graphql` isn't a subgraph. Built once per
    /// [`rebuild`](SchemaRegistry::rebuild), from the scoped schema a
    /// caller granted on exactly those bounded contexts is served.
    pub async fn published(
        &self,
        state: &GraphqlState,
    ) -> skilj_core::error::Result<Option<Arc<PublishedDescription>>> {
        let Some(options) = &state.federation else {
            return Ok(None);
        };
        let cached = |generation| {
            self.published
                .lock()
                .unwrap()
                .as_ref()
                .filter(|(built_at, _)| *built_at == generation)
                .map(|(_, published)| published.clone())
        };
        let generation = self.generation.load(Ordering::Acquire);
        if let Some(published) = cached(generation) {
            return Ok(Some(published));
        }
        let _building = self.publishing.lock().await;
        let generation = self.generation.load(Ordering::Acquire);
        if let Some(published) = cached(generation) {
            return Ok(Some(published));
        }
        let visible: BTreeSet<String> = skilj_core::db::list_bounded_contexts(&state.pool)
            .await?
            .into_iter()
            .filter(|bc| bc.template.is_none() && options.published().contains(&bc.name))
            .map(|bc| bc.name)
            .collect();
        let schema = self.scoped(state, visible).await?;
        let sdl = Arc::new(crate::federation::published_sdl(&schema));
        let published = Arc::new(PublishedDescription {
            service: crate::federation::service_schema(sdl.clone()),
            sdl,
        });
        if self.generation.load(Ordering::Acquire) == generation {
            *self.published.lock().unwrap() = Some((generation, published.clone()));
        }
        Ok(Some(published))
    }

    /// [`published`](SchemaRegistry::published)'s SDL alone.
    pub async fn published_sdl(
        &self,
        state: &GraphqlState,
    ) -> skilj_core::error::Result<Option<Arc<String>>> {
        Ok(self.published(state).await?.map(|p| p.sdl.clone()))
    }

    /// Rebuilds from scratch and atomically swaps in the result. `state`
    /// is passed fresh by the caller each time rather than stored on
    /// `Self` - nothing here needs to remember it between calls, and
    /// storing it would just be a second copy of what every caller
    /// already holds (`Skilj` itself, via `skilj::cross_instance`'s
    /// dispatch loop).
    pub async fn rebuild(&self, state: GraphqlState) -> skilj_core::error::Result<()> {
        let schema = build(state, None).await?;
        self.current.store(Arc::new(schema));
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.scoped.lock().unwrap().clear();
        *self.published.lock().unwrap() = None;
        Ok(())
    }
}

/// `Query`, `Mutation` and `Subscription`. `has_projections` adds the
/// fields that answer with a projection's generated type, which only
/// exist once one does. `federated` marks every root field a grant
/// doesn't face at read or write level `@inaccessible`
/// (docs/architecture.md §194) - `admin` below, and the complement of
/// `federation::PUBLISHED_ROOT_FIELDS`.
fn root_types(
    n: &Naming,
    federated: bool,
    has_projections: bool,
) -> (Object, Object, Subscription) {
    let admin = |field: Field| {
        if federated {
            field.inaccessible()
        } else {
            field
        }
    };

    let mut query = Object::new("Query")
        .field(admin(resolvers::bounded_context_directory::field(n)))
        .field(admin(resolvers::type_registration::projections_field(n)))
        .field(admin(
            resolvers::type_registration::scheduled_event_types_field(n),
        ))
        .field(admin(resolvers::type_registration::event_types_field(n)))
        .field(admin(resolvers::type_registration::command_types_field(n)))
        .field(admin(resolvers::event_query::query_events_field(n)))
        .field(admin(resolvers::event_query::count_events_field(n)))
        .field(admin(resolvers::event_query::inspect_event_field(n)))
        .field(resolvers::event_query::epoch_field(n))
        .field(admin(resolvers::snapshot_query::inspect_snapshot_field(n)))
        .field(admin(resolvers::command_query::fetch_commands_field(n)))
        .field(admin(resolvers::command_submission::dry_run_command_field(
            n,
        )))
        .field(resolvers::private_field_grant_management::list_private_field_grants_field(n))
        .field(admin(
            resolvers::parked_deliveries::parked_deliveries_field(n),
        ))
        .field(resolvers::projection_query::schema_field(n));
    if has_projections {
        query = query.field(resolvers::projection_query::field(n));
    }
    let mutation = Object::new("Mutation")
        .field(admin(resolvers::superadmin_bootstrap::field(n)))
        .field(admin(resolvers::access_management::create_role_field(n)))
        .field(admin(resolvers::access_management::revoke_role_field(n)))
        .field(admin(
            resolvers::access_management::grant_role_access_mapping_field(n),
        ))
        .field(admin(
            resolvers::access_management::revoke_role_access_mapping_field(n),
        ))
        .field(admin(resolvers::bounded_context_creation::field(n)))
        .field(admin(
            resolvers::bounded_context_templating::create_bounded_context_from_template_field(n),
        ))
        .field(admin(
            resolvers::bounded_context_templating::resync_bounded_context_from_template_field(n),
        ))
        .field(admin(resolvers::bounded_context_archival::field(n)))
        .field(admin(resolvers::bounded_context_deletion::field(n)))
        .field(admin(
            resolvers::type_registration::register_event_type_field(n),
        ))
        .field(admin(
            resolvers::type_registration::register_command_type_field(n),
        ))
        .field(admin(
            resolvers::type_registration::register_projection_field(n),
        ))
        .field(admin(
            resolvers::type_registration::rebuild_projection_field(n),
        ))
        .field(admin(
            resolvers::type_registration::discard_projection_rebuild_field(n),
        ))
        .field(admin(
            resolvers::parked_deliveries::retry_parked_delivery_field(n),
        ))
        .field(admin(
            resolvers::parked_deliveries::discard_parked_delivery_field(n),
        ))
        .field(admin(
            resolvers::event_type_admin_operations::create_external_event_token_field(n),
        ))
        .field(admin(
            resolvers::event_type_admin_operations::create_direct_creation_token_field(n),
        ))
        .field(admin(
            resolvers::event_type_admin_operations::create_event_read_token_field(n),
        ))
        .field(admin(
            resolvers::command_type_admin_operations::create_command_token_field(n),
        ))
        .field(admin(resolvers::token_revocation::revoke_token_field(n)))
        .field(resolvers::command_submission::submit_command_field(n))
        .field(admin(resolvers::subject_erasure::field(n)))
        .field(
            resolvers::private_field_grant_management::grant_private_field_access_for_event_field(
                n,
            ),
        )
        .field(
            resolvers::private_field_grant_management::grant_private_field_access_for_command_field(
                n,
            ),
        )
        .field(resolvers::private_field_grant_management::revoke_private_field_access_field(n));
    let mut subscription = Subscription::new("Subscription")
        .field(resolvers::event_subscription::all_events_field(n))
        .field(resolvers::event_subscription::events_by_type_field(n));
    if has_projections {
        subscription =
            subscription.field(resolvers::projection_subscription::projection_updates_field(n));
    }
    (query, mutation, subscription)
}

/// Every type of the schema that doesn't depend on what's registered:
/// all but the root types and `ProjectionQuery`'s generated ones.
fn static_types(n: &Naming) -> Vec<Type> {
    vec![
        gql_types::filter_input(n).into(),
        gql_types::filter_operator_enum(n).into(),
        gql_types::role_object(n).into(),
        gql_types::created_superadmin_object(n).into(),
        gql_types::role_access_mapping_object(n).into(),
        gql_types::context_creator_object(n).into(),
        gql_types::bounded_context_object(n).into(),
        gql_types::role_status_enum(n).into(),
        gql_types::access_level_enum(n).into(),
        gql_types::bounded_context_status_enum(n).into(),
        gql_types::missed_occurrence_policy_enum(n).into(),
        gql_types::event_read_start_position_enum(n).into(),
        gql_types::access_token_status_enum(n).into(),
        gql_types::projection_rebuild_status_enum(n).into(),
        gql_types::tag_mapping_object(n).into(),
        gql_types::tag_mapping_input(n).into(),
        gql_types::sensitive_field_object(n).into(),
        gql_types::sensitive_field_input(n).into(),
        gql_types::private_field_kind_enum(n).into(),
        gql_types::private_field_object(n).into(),
        gql_types::private_field_input(n).into(),
        gql_types::private_field_grant_object(n).into(),
        gql_types::event_type_object(n).into(),
        gql_types::command_type_object(n).into(),
        gql_types::projection_object(n).into(),
        gql_types::projection_rebuild_object(n).into(),
        gql_types::projection_registration_result_object(n).into(),
        gql_types::external_event_token_object(n).into(),
        gql_types::direct_creation_token_object(n).into(),
        gql_types::event_read_token_object(n).into(),
        gql_types::command_token_object(n).into(),
        gql_types::access_token_union(n).into(),
        gql_types::tag_input(n).into(),
        gql_types::queried_event_object(n).into(),
        gql_types::queried_command_object(n).into(),
        gql_types::matching_event_object(n).into(),
        gql_types::event_origin_object(n).into(),
        gql_types::event_meta_object(n).into(),
        gql_types::inspected_event_object(n).into(),
        gql_types::inspected_snapshot_object(n).into(),
        gql_types::submit_command_payload_object(n).into(),
        gql_types::dry_run_command_payload_object(n).into(),
        gql_types::would_be_event_object(n).into(),
        gql_types::encryption_key_object(n).into(),
        gql_types::encryption_key_status_enum(n).into(),
        gql_types::parked_delivery_object(n).into(),
        gql_types::parked_delivery_kind_enum(n).into(),
    ]
}

/// The static types no published root field reaches, before the prefix:
/// what a federated schema marks `@inaccessible` with the admin root
/// fields (docs/architecture.md §194). Fixed at compile time, so worked
/// out once, from a schema of the static types alone - the generated
/// projection types are all reached through `projection`, which is
/// published.
fn admin_only_types() -> &'static HashSet<String> {
    static ADMIN_ONLY: std::sync::OnceLock<HashSet<String>> = std::sync::OnceLock::new();
    ADMIN_ONLY.get_or_init(|| {
        let n = Naming::default();
        let (query, mutation, subscription) = root_types(&n, false, false);
        let mut builder = Schema::build(
            query.type_name(),
            Some(mutation.type_name()),
            Some(subscription.type_name()),
        )
        .register(query)
        .register(mutation)
        .register(subscription);
        for ty in static_types(&n) {
            builder = builder.register(ty);
        }
        let schema = builder
            .finish()
            .expect("the static schema is fixed at compile time");
        crate::federation::unreachable_from_published(&schema.sdl(), &n)
            .expect("async-graphql's own SDL parses")
    })
}

/// Builds one schema value from scratch - [`SchemaRegistry::build`]/
/// [`SchemaRegistry::rebuild`]'s shared implementation, not called
/// directly from outside this module. `state` is baked into the
/// schema's own global `.data()` (not per-request - `pool`/
/// `bootstrap_secret`/`identity` never change once built), so every
/// resolver reaches it via `ctx.data::<GraphqlState>()`. The per-request
/// caller `Role` is the one thing that *does* vary per request -
/// `graphql_handler` injects that separately, via
/// `async_graphql::Request::data`, not here.
///
/// `async`, returning `Result`: `ProjectionQuery`'s own per-projection
/// types (`crate::projection_types::build`) need to list every
/// registered projection across every bounded context, a real query this
/// function makes every time it runs - cheap enough to pay again on
/// every rebuild, not worth caching separately from the schema it feeds.
/// `None` (nothing registered anywhere yet) omits the `projection` field
/// and the `ProjectionResult` union entirely, rather than registering an
/// invalid zero-member union.
///
/// `visible` limits the projection types to those bounded contexts'
/// (docs/architecture.md §138); `None` includes all of them.
async fn build(
    state: GraphqlState,
    visible: Option<&BTreeSet<String>>,
) -> skilj_core::error::Result<Schema> {
    let federated = state.federation.is_some();
    let n = &state.naming.clone();
    let projection_types = projection_types::build(&state.pool, visible, n, federated).await?;
    let (query, mutation, subscription) = root_types(n, federated, projection_types.is_some());
    // docs/architecture.md §194: types only the admin surface reaches are
    // kept out of the supergraph with it.
    let hidden: HashSet<String> = if federated {
        admin_only_types().iter().map(|name| n.ty(name)).collect()
    } else {
        HashSet::new()
    };

    let mut builder = Schema::build(
        query.type_name(),
        Some(mutation.type_name()),
        Some(subscription.type_name()),
    )
    .register(query)
    .register(mutation)
    .register(subscription);
    for ty in static_types(n) {
        builder = builder.register(crate::federation::hide_if(ty, &hidden));
    }

    // ProjectionQuery's own per-projection types (§5.1) - data-dependent,
    // unlike everything registered above, which is why this is the one
    // part of this function that can genuinely fail at runtime (see the
    // .expect() below, which no longer covers this).
    if let Some((objects, enums, union, admitted)) = projection_types {
        for object in objects {
            builder = builder.register(object);
        }
        for enum_type in enums {
            builder = builder.register(enum_type);
        }
        builder = builder.register(union).data(admitted);
    }

    if federated {
        builder = builder
            .enable_federation()
            .entity_resolver(resolvers::projection_query::entity_resolver);
    }

    // Wraps every resolved field, across all 17 resolver modules, in a
    // `tracing` span - the `tracing`-backed extension, deliberately not
    // `async_graphql::extensions::OpenTelemetry` (which talks to the OTel
    // SDK directly), so this crate stays on the same "emit via `tracing`
    // only" boundary every other library crate here keeps. See
    // docs/architecture.md's tracing section.
    builder = builder.extension(async_graphql::extensions::Tracing);
    // docs/architecture.md §72 - see `crate::limits`.
    builder = builder
        .limit_depth(state.limits.max_depth)
        .limit_complexity(state.limits.max_complexity)
        .extension(crate::limits::ExpensiveFieldBudget {
            max: state.limits.max_expensive_fields,
        });

    Ok(builder.data(state).finish().expect(
        "every type registered above this point is fixed at compile time; every one \
         registered from projection_types is named via projection_types::graphql_type_name, \
         the single source both registration and resolution agree on - a SchemaError here \
         would mean a bug in one of those two modules, not a runtime condition",
    ))
}
