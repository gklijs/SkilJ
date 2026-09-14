//! SkilJ: a Rust library for building event-sourced, DDD-style
//! applications backed by a single-instance Postgres database, exposing a
//! GraphQL surface and a REST surface. See `specs/skilj.allium` for the
//! full behavioural specification and `docs/architecture.md` for how
//! it's built.
//!
//! This crate is a thin facade over `skilj-core` + `skilj-graphql` +
//! `skilj-rest`, for the common case of wanting all three. A consumer
//! wanting only one surface can depend on `skilj-core` plus the relevant
//! surface crate directly instead - see docs/architecture.md §3.1.
//!
//! This is also where the type-erasure boundary docs/architecture.md
//! §1.7 describes actually lives: `SkiljBuilder::event_type::<T>()`/
//! `command_type::<T>()`/`projection::<T>()` each capture a plain data
//! record (name, `schemars`-derived JSON Schema, etc.) plus - for command
//! types - one boxed decider closure that bridges a raw stored payload
//! and this bounded context's raw `Event`s into `T::decide()`. See
//! `RegisteredCommandType`'s own doc comment for the closure itself.

use futures_util::stream::{self, StreamExt, TryStreamExt};
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::KeyValue;
use skilj_core::access_control::{AccessLevel, JwksCache, RevocationBroadcaster, Role};
use skilj_core::bootstrap::BootstrapSecret;
use skilj_core::db::Pool;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{Error as EventStoreError, Event, EventBroadcaster};
use skilj_core::plugin::{BoundedContextEvent, CommandDispatcher};
use skilj_core::projections::{ProjectionRebuildStatus, ProjectionRegistration};
use skilj_core::shared::CommandDecision;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use tracing::Instrument;

/// See `skilj-core::db`'s own `meter()`/`LazyLock` doc comment for the
/// `global::meter()` snapshot-binding caveat both instruments below are
/// subject to - only ever touched from inside the two background loops
/// `SkiljBuilder::build` spawns, strictly after a real consuming app's
/// `init_telemetry` has already run.
static BACKGROUND_TASK_TICK_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    opentelemetry::global::meter("skilj")
        .f64_histogram("skilj.background_task.tick.duration")
        .with_unit("s")
        .with_description("Duration of one tick of a skilj background task.")
        .build()
});

static BACKGROUND_TASK_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter("skilj")
        .u64_counter("skilj.background_task.errors")
        .with_description("Errors encountered by a skilj background task.")
        .build()
});

/// Codeberg issue #15: how many bounded contexts `SkiljBuilder::build()`'s
/// own startup warm-up loop and each background poller tick work on at
/// once, rather than one at a time in a plain sequential `for` loop -
/// each bounded context's own work is fully independent (its own
/// Postgres schema, no shared mutable state with any other), so this is
/// purely a throughput knob, not a correctness one. A fixed constant for
/// now, not a `SkiljBuilder` tunable - the "lower-risk incremental fix"
/// this issue's own open questions asked about; easy to expose as a
/// builder option later if a real need for tuning it ever comes up.
const BACKGROUND_TASK_CONCURRENCY: usize = 16;

/// Re-exported so a crate using `#[auto_register]` (whose expansion emits
/// `::skilj::inventory::submit! { ... }`) needs only its existing `skilj`
/// dependency - not a direct one on `inventory` too. Not meant to be used
/// directly by hand-written code; `SkiljBuilder::auto_register()` is the
/// intended entry point.
pub use inventory;
pub use skilj_core::access_control::{IdpConfig, SigningAlgorithm};
pub use skilj_core::encryption::EncryptionMasterKey;
pub use skilj_core::plugin::{
    requires_role, CommandType, CrossContextRoute, EventType, Projection, Snapshot,
    DEFAULT_BOUNDED_CONTEXT,
};
/// See `skilj_macros::auto_register`'s own doc comment - unlike
/// `requires_role` above, this one is facade-specific (its expansion
/// names `EventTypeRegistrar`/`CommandTypeRegistrar`/`ProjectionRegistrar`/
/// `SnapshotRegistrar` below), so it's re-exported here rather than through
/// `skilj_core::plugin`.
pub use skilj_macros::auto_register;

/// `Skilj`'s own bundle of an `IdpConfig` and the `JwksCache` verifying
/// against it - built once, at `.build()` time, and held for the
/// process's lifetime, matching `JwksCache`'s own doc comment: its
/// cached keys and refetch timer need to persist across requests, not
/// be rebuilt per request. `Arc`-wrapped so `graphql_router()` can hand
/// a cheap clone to `skilj-graphql`'s auth extractor without `Skilj`
/// itself needing to live behind an `Arc` - same reasoning
/// `command_dispatcher()` already uses for `Dispatcher`.
struct IdentityProvider {
    config: IdpConfig,
    cache: Arc<JwksCache>,
}

/// Entry point - see docs/architecture.md §1.5 for the full worked
/// example and the reasoning behind every choice below.
pub struct Skilj {
    pool: Pool,
    /// `Arc`-wrapped so `rest_router()`/`graphql_router()` can hand out a
    /// cheap `Arc<dyn CommandDispatcher>` without `Skilj` itself needing
    /// to live behind an `Arc` - see `Dispatcher` below, the trait's own
    /// implementer.
    command_types: Arc<HashMap<(String, String), RegisteredCommandType>>,
    /// `Arc`-wrapped for the identical reason `command_types` is -
    /// `projection_dispatcher()` hands out a cheap `Arc<dyn
    /// ProjectionDispatcher>` without `Skilj` itself needing to live
    /// behind an `Arc`.
    projections: Arc<HashMap<(String, String), RegisteredProjection>>,
    /// `Arc`-wrapped for the identical reason `command_types`/`projections`
    /// are - `event_dispatcher()` hands out a cheap `Arc<dyn
    /// EventDispatcher>`, and the background scheduler task spawned in
    /// `.build()` holds its own clone for the process's lifetime.
    event_types: Arc<HashMap<(String, String), RegisteredEventType>>,
    /// `Arc`-wrapped for the identical reason `command_types`/
    /// `projections`/`event_types` are - `snapshot_dispatcher()` hands
    /// out a cheap `Arc<dyn SnapshotDispatcher>`, and the background
    /// snapshot catch-up task spawned in `.build()` holds its own clone
    /// for the process's lifetime ([docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)).
    snapshots: Arc<HashMap<(String, String), RegisteredSnapshot>>,
    /// `bootstrap::generate_bootstrap_secret`'s output, computed once at
    /// `.build()` time and printed then too (see `SkiljBuilder::build`) -
    /// `None` once an active superadmin already exists
    /// (`ClosesPermanentlyOnFirstClaim`). Read only by
    /// `skilj-graphql`'s `createSuperadmin` mutation resolver.
    bootstrap_secret: Option<BootstrapSecret>,
    /// `None` when `.identity_provider(...)` was never called - every
    /// GraphQL resolver that needs a caller identity has no way to
    /// authenticate anyone in that case (not a silent bypass: there is
    /// no bearer JWT any request could present that would ever resolve
    /// to a verified subject without a configured IdP to verify it
    /// against).
    identity_provider: Option<Arc<IdentityProvider>>,
    /// `ProjectionQuery`'s own `wait_for_sequence` timeout - see
    /// `SkiljBuilder::projection_query_wait_timeout`'s own doc comment.
    projection_query_wait_timeout: std::time::Duration,
    /// `protect_sensitive_fields`'s own envelope-encryption master key -
    /// see `SkiljBuilder::encryption_master_key`'s own doc comment.
    /// `None` when never configured - fine as long as no bounded context
    /// this process registers ever declares a real `sensitive_fields`
    /// entry; `db::resolve_encryption_keys` is what actually raises the
    /// configuration error the first time one does.
    encryption_master_key: Option<EncryptionMasterKey>,
    /// `EventSubscription`'s own real-time delivery source - see
    /// `SkiljBuilder::event_broadcast_capacity`'s own doc comment. One
    /// shared, process-wide broadcaster, constructed once in `.build()`
    /// and handed to both `rest_router()` (every event-creation route
    /// publishes into it) and `graphql_router()` (`allEvents`/
    /// `eventsByType` subscribe to it) - the same "one dispatcher, not
    /// two" treatment `command_dispatcher`/`projection_dispatcher`
    /// already get.
    event_broadcaster: EventBroadcaster,
    /// `EventSubscription`'s own second broadcast channel (drift audit
    /// finding #4) - see `access_control::RevocationBroadcaster`'s own
    /// doc comment for the full design. One shared, process-wide
    /// instance, the same "constructed once in `.build()`, handed to
    /// `graphql_router()`" treatment `event_broadcaster` gets (`rest_router()`
    /// has no use for it - `AccessManagement`, the only surface that
    /// publishes to it, and `EventSubscription`, the only surface that
    /// subscribes, are both GraphQL-only).
    revocation_broadcaster: RevocationBroadcaster,
    /// The in-memory per-bounded-context event cache (drift audit
    /// finding #8) - see `skilj_core::event_cache`'s own module doc
    /// comment for the full design. One shared, process-wide cache,
    /// constructed and warmed once in `.build()`, handed to both
    /// `rest_router()` and `graphql_router()` - the same "one instance,
    /// not two" treatment `event_broadcaster` already gets, and for the
    /// identical reason: every commit needs to reach the one cache every
    /// read consults, regardless of which surface produced it.
    event_cache: EventCache,
    /// The live GraphQL schema (Codeberg issue #2; `@guarantee
    /// RegistrationReachesEveryInstance` in specs/skilj.allium) - built
    /// once in `.build()`, the same "one shared, process-wide thing"
    /// register `event_broadcaster`/`revocation_broadcaster`/`event_cache`
    /// already live in, but kept rebuildable: the background task
    /// `.build()` spawns (see `cross_instance` below) calls
    /// `SchemaRegistry::rebuild` on every `skilj_registration_changed`
    /// notification, from this process's own registration mutations or
    /// another instance's. `Arc`-wrapped so both `graphql_router()` and
    /// that background task can hold a cheap clone.
    schema_registry: Arc<skilj_graphql::schema::SchemaRegistry>,
    /// Codeberg issue #13 - see `skilj_core::template_cache`'s own doc
    /// comment. Already cheaply `Clone` (`Arc`-wrapped internals), so
    /// unlike `schema_registry` this doesn't need an outer `Arc` of its
    /// own - the background cross-instance listener task and
    /// `graphql_state()` both just hold their own clone, refreshed on
    /// every `RegistrationChanged` notification alongside
    /// `schema_registry.rebuild`, and also refreshed synchronously by
    /// `createBoundedContextFromTemplate`'s own resolver (ultra-review
    /// bug_001).
    template_cache: skilj_core::template_cache::TemplateCache,
}

/// `CommandDispatcher`'s own implementer - a thin wrapper around the
/// registry `Skilj` holds, kept as its own type rather than implementing
/// the trait on `Skilj` directly so `rest_router()` can produce an
/// `Arc<dyn CommandDispatcher>` cheaply (cloning the `Arc<HashMap<...>>`
/// alone) without requiring an `Arc<Skilj>`.
struct Dispatcher {
    command_types: Arc<HashMap<(String, String), RegisteredCommandType>>,
    template_cache: skilj_core::template_cache::TemplateCache,
}

impl CommandDispatcher for Dispatcher {
    fn dispatch(
        &self,
        bounded_context: &str,
        command_type: &str,
        payload: &str,
        matching_events: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .command_types
            .get(&(bounded_context, command_type.to_string()))?;
        Some((registered.decide)(payload, matching_events))
    }

    fn required_role(
        &self,
        bounded_context: &str,
        command_type: &str,
    ) -> Option<Option<&'static str>> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .command_types
            .get(&(bounded_context, command_type.to_string()))?;
        Some(registered.required_role)
    }

    fn snapshot_name(
        &self,
        bounded_context: &str,
        command_type: &str,
    ) -> Option<Option<&'static str>> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .command_types
            .get(&(bounded_context, command_type.to_string()))?;
        Some(registered.snapshot_name)
    }

    fn dispatch_from_snapshot(
        &self,
        bounded_context: &str,
        command_type: &str,
        payload: &str,
        snapshot_state_json: &str,
        events_since_snapshot: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .command_types
            .get(&(bounded_context, command_type.to_string()))?;
        Some((registered.decide_from_snapshot)(
            payload,
            snapshot_state_json,
            events_since_snapshot,
        ))
    }
}

/// `ProjectionDispatcher`'s own implementer - same shape and reasoning
/// as `Dispatcher` above, over the `projections` registry instead of
/// `command_types`.
struct ProjectionDispatcherImpl {
    projections: Arc<HashMap<(String, String), RegisteredProjection>>,
    template_cache: skilj_core::template_cache::TemplateCache,
}

impl ProjectionDispatcherImpl {
    /// The one `effective_bounded_context` + registry lookup every
    /// accessor below needs, shared rather than repeated per accessor -
    /// each of `keys`/`project`/`default_state`/`owner_tag_key`/
    /// `team_only` becomes a one-line wrapper around this plus whatever
    /// it does with the field it wants, so the next per-projection
    /// config knob added to `ProjectionDispatcher` costs one small
    /// method here, not another full copy of the lookup.
    fn registered(
        &self,
        bounded_context: &str,
        projection_name: &str,
    ) -> Option<&RegisteredProjection> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        self.projections
            .get(&(bounded_context, projection_name.to_string()))
    }
}

impl skilj_core::plugin::ProjectionDispatcher for ProjectionDispatcherImpl {
    fn keys(
        &self,
        bounded_context: &str,
        projection_name: &str,
        event: &Event,
    ) -> Option<Vec<String>> {
        Some((self.registered(bounded_context, projection_name)?.keys)(
            event,
        ))
    }

    fn project(
        &self,
        bounded_context: &str,
        projection_name: &str,
        state_json: &str,
        event: &Event,
        key: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        Some((self.registered(bounded_context, projection_name)?.project)(state_json, event, key))
    }

    fn default_state(&self, bounded_context: &str, projection_name: &str) -> Option<String> {
        Some(
            self.registered(bounded_context, projection_name)?
                .default_state_json
                .clone(),
        )
    }

    fn owner_tag_key(
        &self,
        bounded_context: &str,
        projection_name: &str,
    ) -> Option<Option<&'static str>> {
        Some(
            self.registered(bounded_context, projection_name)?
                .owner_tag_key,
        )
    }

    fn team_only(
        &self,
        bounded_context: &str,
        projection_name: &str,
    ) -> Option<Option<&'static str>> {
        Some(self.registered(bounded_context, projection_name)?.team_only)
    }
}

/// `EventDispatcher`'s own implementer - same shape and reasoning as
/// `Dispatcher`/`ProjectionDispatcherImpl` above, over the `event_types`
/// registry instead.
struct EventDispatcherImpl {
    event_types: Arc<HashMap<(String, String), RegisteredEventType>>,
    template_cache: skilj_core::template_cache::TemplateCache,
}

impl skilj_core::plugin::EventDispatcher for EventDispatcherImpl {
    fn scheduled_payload(&self, bounded_context: &str, event_type: &str) -> Option<String> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .event_types
            .get(&(bounded_context, event_type.to_string()))?;
        Some((registered.scheduled_payload)())
    }
}

/// `SnapshotDispatcher`'s own implementer - same shape and reasoning as
/// `Dispatcher`/`ProjectionDispatcherImpl`/`EventDispatcherImpl` above,
/// over the `snapshots` registry instead. `snapshot_names` is the one
/// method with no direct sibling on the other three dispatchers - see
/// its own doc comment on the trait for why `Snapshot` needs an
/// enumeration method at all.
struct SnapshotDispatcherImpl {
    snapshots: Arc<HashMap<(String, String), RegisteredSnapshot>>,
    template_cache: skilj_core::template_cache::TemplateCache,
}

impl skilj_core::plugin::SnapshotDispatcher for SnapshotDispatcherImpl {
    fn snapshot_names(&self, bounded_context: &str) -> Vec<&'static str> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        self.snapshots
            .iter()
            .filter(|((bc, _), _)| *bc == bounded_context)
            .map(|(_, registered)| registered.name)
            .collect()
    }

    fn tag_key(&self, bounded_context: &str, snapshot_name: &str) -> Option<&'static str> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .snapshots
            .get(&(bounded_context, snapshot_name.to_string()))?;
        Some(registered.tag_key)
    }

    fn owner_tag_key(
        &self,
        bounded_context: &str,
        snapshot_name: &str,
    ) -> Option<Option<&'static str>> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .snapshots
            .get(&(bounded_context, snapshot_name.to_string()))?;
        Some(registered.owner_tag_key)
    }

    fn version(&self, bounded_context: &str, snapshot_name: &str) -> Option<u64> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .snapshots
            .get(&(bounded_context, snapshot_name.to_string()))?;
        Some(registered.version)
    }

    fn fold(
        &self,
        bounded_context: &str,
        snapshot_name: &str,
        state_json: &str,
        event: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .snapshots
            .get(&(bounded_context, snapshot_name.to_string()))?;
        Some((registered.fold)(state_json, event))
    }

    fn default_state(&self, bounded_context: &str, snapshot_name: &str) -> Option<String> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .snapshots
            .get(&(bounded_context, snapshot_name.to_string()))?;
        Some(registered.default_state_json.clone())
    }
}

/// `CrossContextRouteDispatcher`'s own implementer - a thin wrapper
/// around the registry `SkiljBuilder::build()` builds and hands to the
/// one background task that ever reads it (docs/architecture.md's own
/// write-up of this pass). Deliberately not template-aware
/// (`template_cache`-free, unlike `SnapshotDispatcherImpl`/
/// `ProjectionDispatcherImpl` above) - a route's own `Source`/`Target`
/// bounded contexts are fixed Rust constants, not resolved per request
/// the way a caller-supplied bounded context name is elsewhere.
struct CrossContextRouteDispatcherImpl {
    routes: Arc<HashMap<String, RegisteredCrossContextRoute>>,
}

impl skilj_core::plugin::CrossContextRouteDispatcher for CrossContextRouteDispatcherImpl {
    fn routes(&self) -> Vec<skilj_core::plugin::CrossContextRouteInfo> {
        self.routes.values().map(|r| r.info).collect()
    }

    fn route(
        &self,
        route_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<String>, serde_json::Error>> {
        let registered = self.routes.get(route_name)?;
        Some((registered.route)(source_payload_json))
    }
}

impl Skilj {
    pub fn builder(database_url: impl Into<String>) -> SkiljBuilder {
        SkiljBuilder {
            database_url: database_url.into(),
            current_bounded_context: skilj_core::plugin::DEFAULT_BOUNDED_CONTEXT.to_string(),
            reconciliation_role: None,
            identity_provider: None,
            event_types: HashMap::new(),
            command_types: HashMap::new(),
            projections: HashMap::new(),
            snapshots: HashMap::new(),
            cross_context_routes: HashMap::new(),
            async_projection_poll_interval: std::time::Duration::from_millis(500),
            snapshot_poll_interval: std::time::Duration::from_millis(500),
            cross_context_route_poll_interval: std::time::Duration::from_millis(500),
            scheduler_poll_interval: std::time::Duration::from_secs(1),
            projection_query_wait_timeout: std::time::Duration::from_secs(5),
            encryption_master_key: None,
            event_broadcast_capacity: 1024,
            event_cache_warm_up_count: 1000,
            pool_options: None,
        }
    }

    /// The type-erased `CommandDispatcher` this `Skilj` hands
    /// `rest_router()` internally, exposed directly too - for a consumer
    /// wiring its own GraphQL mutation resolver needing to check
    /// `CommandDispatcher::required_role` before dispatching (§1.3.1,
    /// once `CommandSubmission` itself is built - Phase 3, not this
    /// pass), or any other surface reaching for the same registry
    /// without going through `rest_router()`'s `axum::Router`. Cheap:
    /// cloning the `Arc<HashMap<...>>` alone, the same reasoning
    /// `Dispatcher`'s own doc comment gives.
    pub fn command_dispatcher(&self) -> Arc<dyn CommandDispatcher> {
        Arc::new(Dispatcher {
            command_types: self.command_types.clone(),
            template_cache: self.template_cache.clone(),
        })
    }

    /// The type-erased `ProjectionDispatcher` this `Skilj` hands
    /// `rest_router()`/`graphql_router()` internally - `db::
    /// insert_event_and_update_sync_projections`'s own bridge into a
    /// bounded context's registered `project()` implementations ([§8](../../docs/architecture.md#open-for-a-future-pass)
    /// item 6). Same cheap-`Arc`-clone reasoning as `command_dispatcher()`.
    pub fn projection_dispatcher(&self) -> Arc<dyn skilj_core::plugin::ProjectionDispatcher> {
        Arc::new(ProjectionDispatcherImpl {
            projections: self.projections.clone(),
            template_cache: self.template_cache.clone(),
        })
    }

    /// The type-erased `EventDispatcher` this `Skilj` hands the
    /// background scheduler task `.build()` spawns - `db::
    /// fire_system_event`'s own bridge into a bounded context's
    /// registered `EventType::scheduled_payload()`. Same cheap-`Arc`-clone
    /// reasoning as `command_dispatcher()`/`projection_dispatcher()`.
    pub fn event_dispatcher(&self) -> Arc<dyn skilj_core::plugin::EventDispatcher> {
        Arc::new(EventDispatcherImpl {
            event_types: self.event_types.clone(),
            template_cache: self.template_cache.clone(),
        })
    }

    /// The type-erased `SnapshotDispatcher` this `Skilj` hands
    /// `graphql_router()`'s own snapshot-inspection resolver and the
    /// background snapshot catch-up task `.build()` spawns
    /// ([docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)). Same cheap-`Arc`-clone reasoning as
    /// `command_dispatcher()`/`projection_dispatcher()`/`event_dispatcher()`.
    pub fn snapshot_dispatcher(&self) -> Arc<dyn skilj_core::plugin::SnapshotDispatcher> {
        Arc::new(SnapshotDispatcherImpl {
            snapshots: self.snapshots.clone(),
            template_cache: self.template_cache.clone(),
        })
    }

    /// The freshly-generated `BootstrapSecret`, printed once to stderr at
    /// `.build()` time (see `SkiljBuilder::build`) and held here too -
    /// for a consuming application that wants it available in-process
    /// (a health-check endpoint, a first-run setup UI) rather than only
    /// scraped from the startup log. `None` once an active superadmin
    /// already exists (`ClosesPermanentlyOnFirstClaim`) - the same case
    /// `bootstrap::generate_bootstrap_secret` itself returns `None` for.
    pub fn bootstrap_secret(&self) -> Option<&str> {
        self.bootstrap_secret.as_ref().map(|s| s.secret.as_str())
    }

    /// `skilj-rest`'s routes, mounted onto a fresh `axum::Router` - see
    /// [docs/architecture.md §7](../../docs/architecture.md#rest-wire-contract). A caller wanting the REST and GraphQL
    /// surfaces combined merges this with `graphql_router()` however
    /// `axum::Router::merge`/`nest` suits their own application.
    pub fn rest_router(&self) -> axum::Router {
        skilj_rest::router(
            self.pool.clone(),
            self.command_dispatcher(),
            self.projection_dispatcher(),
            self.snapshot_dispatcher(),
            self.encryption_master_key.clone(),
            self.event_broadcaster.clone(),
            self.event_cache.clone(),
        )
    }

    /// `skilj-graphql`'s single unified schema, mounted onto a fresh
    /// `axum::Router` - see [docs/architecture.md §5](../../docs/architecture.md#graphql-wire-contract). A caller wanting the
    /// REST and GraphQL surfaces combined merges this with
    /// `rest_router()` however `axum::Router::merge`/`nest` suits their
    /// own application - see `rest_router()`'s own doc comment.
    ///
    /// Covers all 16 of the spec's GraphQL-facing surfaces: the
    /// superadmin admin console, the `AdminAccess`-gated
    /// type-registration/token surfaces, and all five dynamic
    /// per-bounded-context business surfaces (`EventQuery`/`CommandQuery`/
    /// `CommandSubmission`/`ProjectionQuery`/`EventSubscription`) - the
    /// last of which mounts a GraphQL-over-websocket handler on the same
    /// `/graphql` path (see `skilj_graphql::router`'s own doc comment).
    ///
    /// `async`, returning `Result` for symmetry with `skilj_graphql::router`'s
    /// own signature - building this `GraphqlState`/mounting the router
    /// is itself infallible today (the schema behind it was already
    /// built in `.build()`, not here - see `schema_registry`'s own doc
    /// comment), but nothing about `graphql_router`'s own contract
    /// promises that stays true forever.
    pub async fn graphql_router(&self) -> skilj_core::error::Result<axum::Router> {
        skilj_graphql::router(Arc::clone(&self.schema_registry), self.graphql_state()).await
    }

    /// `GraphqlState`'s one real constructor - `graphql_router()`'s own
    /// call site, plus `.build()`'s (to build the initial
    /// `SchemaRegistry` before `Skilj` even exists to call
    /// `graphql_router()` on) and `cross_instance`'s dispatch loop's (to
    /// rebuild it later). Factored out rather than duplicated three
    /// times, unlike `command_dispatcher()`/`projection_dispatcher()`
    /// above, which only ever had the one caller each.
    fn graphql_state(&self) -> skilj_graphql::GraphqlState {
        skilj_graphql::GraphqlState {
            pool: self.pool.clone(),
            bootstrap_secret: self.bootstrap_secret.clone(),
            identity: self
                .identity_provider
                .as_ref()
                .map(|ip| skilj_graphql::auth::Identity {
                    config: ip.config.clone(),
                    cache: Arc::clone(&ip.cache),
                }),
            dispatcher: self.command_dispatcher(),
            projection_dispatcher: self.projection_dispatcher(),
            snapshot_dispatcher: self.snapshot_dispatcher(),
            projection_query_wait_timeout: self.projection_query_wait_timeout,
            encryption_master_key: self.encryption_master_key.clone(),
            event_broadcaster: self.event_broadcaster.clone(),
            revocation_broadcaster: self.revocation_broadcaster.clone(),
            event_cache: self.event_cache.clone(),
            template_cache: self.template_cache.clone(),
        }
    }
}

/// What `Skilj::builder().build()` did on this startup: which bounded
/// contexts' types registered successfully, which were skipped because
/// the reconciliation Role has no admin access to them yet (expected,
/// not an error - §1.5), and (if `.build()` returned `Err` instead) which
/// registration was genuinely rejected. Each entry is `"bounded_context/name"`.
#[derive(Debug, Default)]
pub struct ReconciliationReport {
    pub registered: Vec<String>,
    pub skipped_no_access: Vec<String>,
}

/// `Dispatcher`'s own `EventDispatcher::scheduled_payload` closure -
/// `T::scheduled_payload()` boxed and JSON-serialised, the identical
/// no-runtime-captures-beyond-the-`Box` shape `DeciderFn`/`ProjectFn`
/// already have. Only ever called (via `db::fire_system_event`) once an
/// occurrence is already confirmed eligible - see `EventType::
/// scheduled_payload`'s own doc comment on why it's safe for this to
/// call straight through to a method that panics if never overridden.
type ScheduledPayloadFn = Box<dyn Fn() -> String + Send + Sync>;

/// A `.event_type::<T>()` call's own captured data - everything
/// `register_event_type` needs except the caller-supplied `access_mapping`/
/// `bounded_context`/`existing`, which reconciliation resolves at
/// `.build()` time, not registration time.
struct RegisteredEventType {
    schema: String,
    tag_mappings: Vec<skilj_core::shared::TagMapping>,
    owner_tag_key: Option<String>,
    sensitive_fields: Vec<skilj_core::shared::SensitiveField>,
    private_fields: Vec<skilj_core::shared::PrivateField>,
    external_creation_allowed: bool,
    direct_creation_allowed: bool,
    event_read_allowed: bool,
    system_triggered_allowed: bool,
    system_triggered_schedule: Option<String>,
    missed_occurrence_policy: Option<skilj_core::event_store::MissedOccurrencePolicy>,
    /// Called by `Dispatcher::scheduled_payload` (this module's own
    /// `EventDispatcher` implementer), reached from the background
    /// scheduler `SkiljBuilder::build()` spawns, through `db::
    /// fire_system_event`.
    scheduled_payload: ScheduledPayloadFn,
}

/// `T: 'static` for the same reason `registered_command_type` needs it -
/// lets the boxed `scheduled_payload` closure satisfy `+ 'static`.
fn registered_event_type<T: EventType + 'static>() -> RegisteredEventType {
    let schema = schemars::schema_for!(T::Payload);
    RegisteredEventType {
        schema: serde_json::to_string(&schema).expect("JSON Schema serialization is infallible"),
        tag_mappings: T::tag_mappings(),
        owner_tag_key: T::owner_tag_key().map(str::to_string),
        sensitive_fields: T::sensitive_fields(),
        private_fields: T::private_fields(),
        external_creation_allowed: T::external_creation_allowed(),
        direct_creation_allowed: T::direct_creation_allowed(),
        event_read_allowed: T::event_read_allowed(),
        system_triggered_allowed: T::system_triggered_allowed(),
        system_triggered_schedule: T::system_triggered_schedule(),
        missed_occurrence_policy: T::missed_occurrence_policy(),
        scheduled_payload: Box::new(|| {
            serde_json::to_string(&T::scheduled_payload())
                .expect("JSON serialization of Payload is infallible")
        }),
    }
}

/// One `Event -> skilj_core::error::Result<CommandDecision>` closure,
/// closing over a single `T: CommandType` alone - no runtime captures, so
/// it costs nothing beyond the `Box` itself. Built once, at
/// `.command_type::<T>()` call time (see `registered_command_type`
/// below), and called by the still-to-be-wired `CommandTrigger` route
/// ([§8](../../docs/architecture.md#open-for-a-future-pass) item 4) once per submission: deserialize the payload into
/// `T::Payload`, convert every matching raw `Event` into `T::Event` via
/// `BoundedContextEvent::try_from_event`, then call `T::decide()`. Either
/// decode step failing surfaces as `EventStoreError::PayloadDecodeFailed`,
/// a library-level error rather than a `CommandDecision::Rejected` - see
/// that variant's own doc comment for why.
type DeciderFn =
    Box<dyn Fn(&str, &[Event]) -> skilj_core::error::Result<CommandDecision> + Send + Sync>;

/// `DeciderFn`'s own twin for `CommandType::decide_from_snapshot` -
/// [docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 2". `(payload_json,
/// snapshot_state_json, events_since_snapshot)` - the same shape
/// `decide_from_snapshot` itself has, just with `payload`/`events_since_snapshot`
/// still type-erased, exactly like `DeciderFn` above.
type DecideFromSnapshotFn =
    Box<dyn Fn(&str, &str, &[Event]) -> skilj_core::error::Result<CommandDecision> + Send + Sync>;

struct RegisteredCommandType {
    schema: String,
    tag_mappings: Vec<skilj_core::shared::TagMapping>,
    owner_tag_key: Option<String>,
    sensitive_fields: Vec<skilj_core::shared::SensitiveField>,
    private_fields: Vec<skilj_core::shared::PrivateField>,
    rest_trigger_allowed: bool,
    /// `CommandType::required_role()`'s value, carried straight through
    /// unchanged - not persisted anywhere (see that method's own doc
    /// comment), read back only by `Dispatcher::required_role` for
    /// `skilj-graphql`'s eventual mutation resolver to check (§1.3.1,
    /// [§8](../../docs/architecture.md#open-for-a-future-pass) item 5).
    required_role: Option<&'static str>,
    /// `CommandType::snapshot()`'s value, carried straight through
    /// unchanged - read back by `Dispatcher::snapshot_name`, the same
    /// "outer/inner `Option`" convention `required_role` already uses.
    snapshot_name: Option<&'static str>,
    /// Called by `Dispatcher::dispatch` (this module's own
    /// `CommandDispatcher` implementer), reached from `skilj-rest`'s
    /// `CommandTrigger` route through the `Arc<dyn CommandDispatcher>`
    /// `rest_router()` hands it - [docs/architecture.md §8](../../docs/architecture.md#open-for-a-future-pass) item 4.
    decide: DeciderFn,
    /// `Dispatcher::dispatch_from_snapshot`'s own bridge into
    /// `CommandType::decide_from_snapshot` - `decide`'s own twin, called
    /// instead of it when `snapshot_name` is `Some` and the caller's own
    /// derived tags matched.
    decide_from_snapshot: DecideFromSnapshotFn,
}

/// `T: 'static` (beyond `CommandType` itself) is what lets the returned
/// closure satisfy `Box<dyn Fn(...) + Send + Sync>`'s implicit `+
/// 'static` - always trivially true for a real `CommandType` impl (a
/// plain, data-free marker struct), never a practical restriction.
fn registered_command_type<T: CommandType + 'static>() -> RegisteredCommandType {
    let schema = schemars::schema_for!(T::Payload);
    RegisteredCommandType {
        schema: serde_json::to_string(&schema).expect("JSON Schema serialization is infallible"),
        tag_mappings: T::tag_mappings(),
        owner_tag_key: T::owner_tag_key().map(str::to_string),
        sensitive_fields: T::sensitive_fields(),
        private_fields: T::private_fields(),
        rest_trigger_allowed: T::rest_trigger_allowed(),
        required_role: T::required_role(),
        snapshot_name: T::snapshot(),
        decide: Box::new(|payload_json, raw_events| {
            let payload: T::Payload = serde_json::from_str(payload_json)
                .map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?;
            let mut matching = Vec::with_capacity(raw_events.len());
            for event in raw_events {
                if let Some(converted) = T::Event::try_from_event(event) {
                    matching.push(
                        converted
                            .map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?,
                    );
                }
            }
            Ok(T::decide(&payload, &matching))
        }),
        decide_from_snapshot: Box::new(|payload_json, snapshot_state_json, raw_events| {
            let payload: T::Payload = serde_json::from_str(payload_json)
                .map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?;
            let mut matching = Vec::with_capacity(raw_events.len());
            for event in raw_events {
                if let Some(converted) = T::Event::try_from_event(event) {
                    matching.push(
                        converted
                            .map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?,
                    );
                }
            }
            Ok(T::decide_from_snapshot(
                &payload,
                snapshot_state_json,
                &matching,
            ))
        }),
    }
}

/// Which instance(s) of a projection's own state one event touches,
/// closing over a single `T: Projection` alone - `T::keys`'s own
/// type-erased bridge, the counterpart `ProjectFn` below has for
/// `T::project`. Returns no keys at all - not `T::keys`'s own default -
/// when `event`'s own type isn't one this projection actually consumes,
/// mirroring `ProjectFn`'s identical "state passes through unchanged"
/// treatment for the same case (see `ProjectionDispatcher::keys`'s own
/// doc comment for why that check can't be left to
/// `BoundedContextEvent::try_from_event` alone).
type KeysFn = Box<dyn Fn(&Event) -> Vec<String> + Send + Sync>;

/// Applies one event to a projection's current state (as JSON), for the
/// one instance named by `key` - closing over a single `T: Projection`
/// alone, no runtime captures beyond the `Box` itself, the same shape
/// `DeciderFn` above already has. Built once, at `.projection::<T>()`
/// call time (see `registered_projection` below), called by `db::
/// insert_event_and_update_sync_projections`/`catch_up_bounded_context`
/// ([§8](../../docs/architecture.md#open-for-a-future-pass) item 6) through the `ProjectionDispatcher` bridge - `Dispatcher`
/// below, this module's own implementer. Returns `state_json` unchanged
/// when `event`'s own type isn't one this projection actually consumes -
/// see `ProjectionDispatcher::project`'s own doc comment for why that
/// check can't be left to `BoundedContextEvent::try_from_event` alone.
type ProjectFn = Box<dyn Fn(&str, &Event, &str) -> skilj_core::error::Result<String> + Send + Sync>;

/// See `RegisteredEventType` above - same shape and reasoning, for
/// `Projection`. `consumed_event_types` carries `EventType::NAME`s only
/// (`&'static str`s from `Projection::consumed_event_types()`) -
/// resolved into full `event_store::EventType`s during reconciliation,
/// once the bounded context's own admin access has already been
/// confirmed (see `reconcile_projections` below). `default_state_json`
/// is what a *new* instance's own `projection_state` row starts from,
/// lazily, the first time any event touches its key ([§9](../../docs/architecture.md#next-steps)'s own "keyed /
/// multi-row Projections" pass - nothing is seeded up front anymore,
/// since a projection's own instances aren't known until events actually
/// name them).
struct RegisteredProjection {
    schema: String,
    consumed_event_types: Vec<&'static str>,
    sync: bool,
    default_state_json: String,
    owner_tag_key: Option<&'static str>,
    team_only: Option<&'static str>,
    keys: KeysFn,
    project: ProjectFn,
}

fn registered_projection<T: Projection + 'static>() -> RegisteredProjection {
    let schema = schemars::schema_for!(T::State);
    let consumed_event_types = T::consumed_event_types();
    let consumed_for_keys = consumed_event_types.clone();
    let consumed_for_project = consumed_event_types.clone();
    let default_state_json = serde_json::to_string(&T::State::default())
        .expect("JSON serialization of a Default::default() State is infallible");
    RegisteredProjection {
        schema: serde_json::to_string(&schema).expect("JSON Schema serialization is infallible"),
        consumed_event_types,
        sync: T::sync(),
        default_state_json,
        owner_tag_key: T::OWNER_TAG_KEY,
        team_only: T::TEAM_ONLY,
        keys: Box::new(move |event| {
            if !consumed_for_keys
                .iter()
                .any(|&name| name == event.event_type.name)
            {
                return Vec::new();
            }
            let Some(Ok(converted)) = T::Event::try_from_event(event) else {
                // Either the generated enum disagrees with
                // consumed_event_types (unreachable in practice - both
                // are derived from the same registered event types), or
                // the stored payload doesn't decode - either way, no
                // instance to report a key for.
                return Vec::new();
            };
            T::keys(&converted)
        }),
        project: Box::new(move |state_json, event, key| {
            if !consumed_for_project
                .iter()
                .any(|&name| name == event.event_type.name)
            {
                return Ok(state_json.to_string());
            }
            let mut state: T::State = serde_json::from_str(state_json)
                .map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?;
            let Some(converted) = T::Event::try_from_event(event) else {
                // consumed_event_types said this event type is one we
                // fold, but the generated enum disagrees - unreachable
                // in practice (both are derived from the same registered
                // event types), so state is left unchanged rather than
                // guessing.
                return Ok(state_json.to_string());
            };
            let converted =
                converted.map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?;
            T::project(&mut state, &converted, key);
            Ok(serde_json::to_string(&state).expect("JSON serialization of State is infallible"))
        }),
    }
}

/// Folds one event into a snapshot's own state (as JSON) - `Snapshot::fold`'s
/// own type-erased bridge, closing over a single `T: Snapshot` alone,
/// the same shape `ProjectFn` above has for `Projection::project`. Built
/// once, at `.snapshot::<T>()` call time (see `registered_snapshot`
/// below), called by `db::catch_up_snapshots` ([docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events))
/// through the `SnapshotDispatcher` bridge - `SnapshotDispatcherImpl`
/// below, this module's own implementer.
///
/// Unlike `ProjectFn`, which silently passes `state_json` through
/// unchanged for an event type a `Projection` doesn't consume (a
/// `Projection` folds several event types, so that's an expected,
/// common case), this is only ever called for an event
/// `catch_up_snapshots` already confirmed carries this snapshot's own
/// `TAG_KEY` - a `BoundedContextEvent::try_from_event` failure here is a
/// real, surfaced `Err`, not a silent no-op (see
/// `SnapshotDispatcher::fold`'s own doc comment).
type SnapshotFoldFn = Box<dyn Fn(&str, &Event) -> skilj_core::error::Result<String> + Send + Sync>;

/// See `RegisteredProjection` above - same shape and reasoning, for
/// `Snapshot`. `name` (`T::NAME`) is carried as its own field, unlike
/// `RegisteredCommandType`/`RegisteredProjection` (whose own `NAME` is
/// already the registry's own map key) - `SnapshotDispatcher::snapshot_names`
/// needs to hand back `&'static str`s it doesn't otherwise have, since
/// there is deliberately no metadata table to enumerate instead (see
/// `skilj_core::plugin::Snapshot`'s own doc comment).
struct RegisteredSnapshot {
    name: &'static str,
    tag_key: &'static str,
    owner_tag_key: Option<&'static str>,
    version: u64,
    default_state_json: String,
    fold: SnapshotFoldFn,
}

fn registered_snapshot<T: Snapshot + 'static>() -> RegisteredSnapshot {
    let default_state_json = serde_json::to_string(&T::State::default())
        .expect("JSON serialization of a Default::default() State is infallible");
    RegisteredSnapshot {
        name: T::NAME,
        tag_key: T::TAG_KEY,
        owner_tag_key: T::OWNER_TAG_KEY,
        version: T::VERSION,
        default_state_json,
        fold: Box::new(|state_json, event| {
            let mut state: T::State = serde_json::from_str(state_json)
                .map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?;
            let converted = T::Event::try_from_event(event).ok_or_else(|| {
                EventStoreError::PayloadDecodeFailed(format!(
                    "event type {} does not convert via this bounded context's own \
                     BoundedContextEvent - snapshot {} can't fold it",
                    event.event_type.name,
                    T::NAME
                ))
            })?;
            let converted =
                converted.map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?;
            T::fold(&mut state, &converted);
            Ok(serde_json::to_string(&state).expect("JSON serialization of State is infallible"))
        }),
    }
}

/// One registered `CrossContextRoute` - the type-erased closure
/// captures `R::route` plus `R::Source::Payload`/`R::Target::Payload`'s
/// own (de)serialization, so `skilj_core::plugin::CrossContextRouteDispatcher::route`
/// (the trait `Skilj` implements over this registry, mirroring
/// `RegisteredSnapshot`'s own shape) needs nothing generic at its own
/// call site.
struct RegisteredCrossContextRoute {
    info: skilj_core::plugin::CrossContextRouteInfo,
    route: fn(&str) -> Result<Option<String>, serde_json::Error>,
}

fn registered_cross_context_route<R: CrossContextRoute + 'static>() -> RegisteredCrossContextRoute {
    RegisteredCrossContextRoute {
        info: skilj_core::plugin::CrossContextRouteInfo {
            name: R::NAME,
            source_bounded_context: R::Source::BOUNDED_CONTEXT,
            source_event_type: R::Source::NAME,
            target_bounded_context: R::Target::BOUNDED_CONTEXT,
            target_command_type: R::Target::NAME,
            start_from: R::START_FROM,
        },
        route: |payload_json| {
            let source_payload: <R::Source as EventType>::Payload =
                serde_json::from_str(payload_json)?;
            match R::route(&source_payload) {
                None => Ok(None),
                Some(target_payload) => Ok(Some(serde_json::to_string(&target_payload)?)),
            }
        },
    }
}

/// One `#[auto_register]`-tagged `EventType` impl's own contribution -
/// the type-erased equivalent of one
/// `.bounded_context(T::BOUNDED_CONTEXT).event_type::<T>()` call, as a
/// plain `fn` pointer (no captures needed, so no `Box<dyn Fn>`). `inventory`
/// collects one of these per macro-tagged impl linked into the binary;
/// `SkiljBuilder::auto_register()` folds every one of them into `self`.
/// See `skilj_macros::auto_register`'s own doc comment for what emits
/// these.
pub struct EventTypeRegistrar(pub fn(SkiljBuilder) -> SkiljBuilder);
inventory::collect!(EventTypeRegistrar);

/// See `EventTypeRegistrar` above - same shape and reasoning, for
/// `#[auto_register]` over a `CommandType` impl.
pub struct CommandTypeRegistrar(pub fn(SkiljBuilder) -> SkiljBuilder);
inventory::collect!(CommandTypeRegistrar);

/// See `EventTypeRegistrar` above - same shape and reasoning, for
/// `#[auto_register]` over a `Projection` impl.
pub struct ProjectionRegistrar(pub fn(SkiljBuilder) -> SkiljBuilder);
inventory::collect!(ProjectionRegistrar);

/// See `EventTypeRegistrar` above - same shape and reasoning, for
/// `#[auto_register]` over a `Snapshot` impl ([docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)).
pub struct SnapshotRegistrar(pub fn(SkiljBuilder) -> SkiljBuilder);
inventory::collect!(SnapshotRegistrar);

pub struct SkiljBuilder {
    database_url: String,
    current_bounded_context: String,
    reconciliation_role: Option<String>,
    identity_provider: Option<IdpConfig>,
    event_types: HashMap<(String, String), RegisteredEventType>,
    command_types: HashMap<(String, String), RegisteredCommandType>,
    projections: HashMap<(String, String), RegisteredProjection>,
    snapshots: HashMap<(String, String), RegisteredSnapshot>,
    /// Keyed by `CrossContextRoute::NAME` alone, not `(bounded_context,
    /// name)` the way `event_types`/`command_types`/`projections`/
    /// `snapshots` are - a route by definition spans two bounded
    /// contexts, so there is no single one to key it against the way
    /// `.bounded_context(...)` scopes every other registration.
    cross_context_routes: HashMap<String, RegisteredCrossContextRoute>,
    async_projection_poll_interval: std::time::Duration,
    snapshot_poll_interval: std::time::Duration,
    cross_context_route_poll_interval: std::time::Duration,
    scheduler_poll_interval: std::time::Duration,
    projection_query_wait_timeout: std::time::Duration,
    encryption_master_key: Option<EncryptionMasterKey>,
    event_broadcast_capacity: usize,
    event_cache_warm_up_count: usize,
    pool_options: Option<skilj_core::db::PgPoolOptions>,
}

impl SkiljBuilder {
    /// Every `event_type`/`command_type`/`projection` call following this
    /// one registers against `name`, until the next `bounded_context`
    /// call changes it. Optional for a single-bounded-context app: every
    /// `SkiljBuilder` already starts scoped to `plugin::DEFAULT_BOUNDED_CONTEXT`
    /// ("default"), so skipping this call entirely registers everything
    /// there.
    pub fn bounded_context(mut self, name: impl Into<String>) -> Self {
        self.current_bounded_context = name.into();
        self
    }

    fn current_bounded_context(&self) -> String {
        self.current_bounded_context.clone()
    }

    /// Applies every `#[auto_register]`-tagged `EventType`/`CommandType`/
    /// `Projection` impl linked into this binary (`skilj_macros::
    /// auto_register`'s own doc comment) - each one registers itself
    /// under its own `T::BOUNDED_CONTEXT`, independent of whatever
    /// `.bounded_context(...)` last set on `self`. Purely additive: safe
    /// to call before, after, or interleaved with manual
    /// `.event_type::<T>()`/`.command_type::<T>()`/`.projection::<T>()`
    /// calls, since every path only ever inserts an entry by its own
    /// `(bounded_context, NAME)` key, the same as any other builder call.
    pub fn auto_register(mut self) -> Self {
        for registrar in inventory::iter::<EventTypeRegistrar> {
            self = (registrar.0)(self);
        }
        for registrar in inventory::iter::<CommandTypeRegistrar> {
            self = (registrar.0)(self);
        }
        for registrar in inventory::iter::<ProjectionRegistrar> {
            self = (registrar.0)(self);
        }
        for registrar in inventory::iter::<SnapshotRegistrar> {
            self = (registrar.0)(self);
        }
        self
    }

    pub fn event_type<T: EventType + 'static>(mut self) -> Self {
        let bc = self.current_bounded_context();
        self.event_types
            .insert((bc, T::NAME.to_string()), registered_event_type::<T>());
        self
    }

    pub fn command_type<T: CommandType + 'static>(mut self) -> Self {
        let bc = self.current_bounded_context();
        self.command_types
            .insert((bc, T::NAME.to_string()), registered_command_type::<T>());
        self
    }

    pub fn projection<T: Projection + 'static>(mut self) -> Self {
        let bc = self.current_bounded_context();
        self.projections
            .insert((bc, T::NAME.to_string()), registered_projection::<T>());
        self
    }

    /// [docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events). Same shape as `.projection::<T>()`
    /// above; unlike it, has no matching GraphQL registration mutation -
    /// see `skilj_core::plugin::Snapshot`'s own doc comment for why.
    pub fn snapshot<T: Snapshot + 'static>(mut self) -> Self {
        let bc = self.current_bounded_context();
        self.snapshots
            .insert((bc, T::NAME.to_string()), registered_snapshot::<T>());
        self
    }

    /// Registers a [`CrossContextRoute`] - unlike `event_type`/
    /// `command_type`/`projection`/`snapshot`, never scoped by the
    /// current `.bounded_context(...)` chain, since a route's own
    /// `Source`/`Target` already each know their own bounded context.
    /// Keyed by `R::NAME` - registering two routes under the same name
    /// silently replaces the first, the same "last registration for a
    /// given key wins" convention every other `HashMap`-backed registry
    /// here already has.
    pub fn cross_context_route<R: CrossContextRoute + 'static>(mut self) -> Self {
        self.cross_context_routes
            .insert(R::NAME.to_string(), registered_cross_context_route::<R>());
        self
    }

    /// The Role the startup reconciliation loop authenticates as - named
    /// by `external_subject`, the same identifier every other identity
    /// resolution in the spec keys on. Optional: omitting it skips
    /// reconciliation entirely and cleanly - see §1.5 for why this is
    /// never a system-identity bypass.
    pub fn reconciliation_role(mut self, external_subject: impl Into<String>) -> Self {
        self.reconciliation_role = Some(external_subject.into());
        self
    }

    /// The trusted external IdP `graphql_router()`'s resolvers
    /// authenticate GraphQL callers against - see docs/architecture.md
    /// [§6](../../docs/architecture.md#idp-trust-configuration). Optional: omitting it means `graphql_router()` still mounts
    /// (`createSuperadmin` needs no caller identity at all - see
    /// `surface SuperadminBootstrap`), but every other resolver can never
    /// authenticate anyone, since there is no bearer JWT any request
    /// could present that would ever resolve to a verified subject
    /// without a configured IdP to verify it against.
    pub fn identity_provider(mut self, config: IdpConfig) -> Self {
        self.identity_provider = Some(config);
        self
    }

    /// How often the single shared background task `.build()` spawns
    /// polls for `sync: false` `Projection`s and `building`
    /// `ProjectionRebuild`s to catch up - `db::catch_up_bounded_context`'s
    /// own wake mechanism ([§8](../../docs/architecture.md#open-for-a-future-pass) item 6, async case). Defaults to 500ms;
    /// there is no event-delivery mechanism to wake it early (see
    /// docs/architecture.md's own write-up of this pass for why), so a
    /// caller wanting tighter freedom between an event committing and its
    /// async projections reflecting it configures this instead - the same
    /// "process-start configuration knob, not hardcoded" register
    /// `await_projection_caught_up`'s own wait bound already lives in.
    pub fn async_projection_poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.async_projection_poll_interval = interval;
        self
    }

    /// How often the single shared background task `.build()` spawns
    /// polls for registered `Snapshot`s to catch up -
    /// `db::catch_up_snapshots`' own wake mechanism (docs/architecture.md
    /// [§19](../../docs/architecture.md#optional-snapshotting-matching-events)), the `Snapshot` equivalent of `async_projection_poll_interval`
    /// above. Defaults to 500ms, matching that default for the identical
    /// reason.
    pub fn snapshot_poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.snapshot_poll_interval = interval;
        self
    }

    /// How often the single shared background task `.build()` spawns
    /// polls every registered [`CrossContextRoute`] to catch up -
    /// `db::catch_up_cross_context_route`'s own wake mechanism, the
    /// `CrossContextRoute` equivalent of `snapshot_poll_interval` above.
    /// Defaults to 500ms, matching that default for the identical
    /// reason.
    pub fn cross_context_route_poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.cross_context_route_poll_interval = interval;
        self
    }

    /// How often the background scheduler task `.build()` spawns checks
    /// every `system_triggered_allowed` `EventType`, across every
    /// bounded context, for a due occurrence - `rule CreateSystemEvent`'s
    /// own wake mechanism, the `EventType` equivalent of
    /// `async_projection_poll_interval` above. Defaults to 1s -
    /// deliberately below a `cron` schedule's own finest grain (whole
    /// seconds), so no occurrence is ever more than one tick late. A
    /// caller with only minute-grained schedules can widen this to cut
    /// idle polling cost.
    pub fn scheduler_poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.scheduler_poll_interval = interval;
        self
    }

    /// How long `ProjectionQuery`'s `waitForSequence` argument blocks
    /// before failing with a distinguishable timeout, when the projection
    /// hasn't caught up to the requested sequence yet - see
    /// `rule QueryProjection`'s own guidance: "a process-start
    /// configuration knob, not a per-query argument or a fixed value".
    /// Defaults to 5s. Costs a sync projection nothing - its
    /// `caught_up_to` is already current the moment its write committed,
    /// so `waitForSequence` is satisfied on the very first poll.
    pub fn projection_query_wait_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.projection_query_wait_timeout = timeout;
        self
    }

    /// `protect_sensitive_fields`'s own envelope-encryption master key -
    /// see `skilj_core::encryption`'s own module doc comment for the full
    /// design. Optional: a bounded context with no real `sensitive_fields`
    /// entry never needs one; declaring a sensitive field and then
    /// actually writing an event/command that references it without one
    /// configured is a genuine, actionable configuration error
    /// (`encryption::Error::MasterKeyNotConfigured`), not a silent bypass.
    /// A consuming application owns keeping this value stable across
    /// restarts - losing it makes every already-provisioned
    /// `EncryptionKey` permanently unrecoverable.
    pub fn encryption_master_key(mut self, master_key: EncryptionMasterKey) -> Self {
        self.encryption_master_key = Some(master_key);
        self
    }

    /// How many not-yet-delivered events a single slow `EventSubscription`
    /// subscriber may lag behind by before its next delivery attempt ends
    /// its stream instead (`RecvError::Lagged` - see
    /// `EventBroadcaster`'s/`resolvers::event_subscription`'s own doc
    /// comments for why that's the correct behaviour, not a bug: no
    /// silent gaps, ever). Defaults to 1024 - the same "sensible default,
    /// opt-in override" register `async_projection_poll_interval` already
    /// lives in.
    pub fn event_broadcast_capacity(mut self, capacity: usize) -> Self {
        self.event_broadcast_capacity = capacity;
        self
    }

    /// The in-memory per-bounded-context event cache's own warm-up fetch
    /// size (drift audit finding #8) - the spec's own "a configurable
    /// count that defaults to 1000... a library/runtime configuration
    /// knob set when the process starts, not a registered or
    /// admin-managed value", the identical register
    /// `scheduler_poll_interval`/`async_projection_poll_interval` already
    /// live in. Also the steady-state cap every bounded context's own
    /// window is held to afterward - see `skilj_core::event_cache`'s own
    /// module doc comment for why those are the same number.
    pub fn event_cache_warm_up_count(mut self, count: usize) -> Self {
        self.event_cache_warm_up_count = count;
        self
    }

    /// Connection pool sizing/timeouts (`max_connections`,
    /// `min_connections`, `acquire_timeout`, `idle_timeout`, ...) -
    /// `sqlx::postgres::PgPoolOptions`, unset by default (`sqlx`'s own
    /// bare default: a 10-connection cap, no configured timeouts).
    /// Worth setting explicitly for real production load: the
    /// background async-projection/snapshot/scheduler pollers this
    /// same `.build()` spawns already compete with every foreground
    /// GraphQL/REST request for whatever this pool provides.
    pub fn pool_options(mut self, options: skilj_core::db::PgPoolOptions) -> Self {
        self.pool_options = Some(options);
        self
    }

    /// Runs the startup reconciliation loop automatically (§1.5). Returns
    /// `Err` only for a genuine registration rejection (e.g. an
    /// incompatible schema change) - a bounded context the reconciliation
    /// Role has no admin access to yet is reported in
    /// `ReconciliationReport`, not an error.
    pub async fn build(self) -> Result<(Skilj, ReconciliationReport), skilj_core::Error> {
        let pool = match self.pool_options {
            Some(options) => skilj_core::db::connect_with(&self.database_url, options).await?,
            None => skilj_core::db::connect(&self.database_url).await?,
        };
        skilj_core::db::migrate(&pool).await?;

        // `default BoundedContext admin`'s own `created_at`/`created_by`
        // stamping - unconditional, every startup, and independent of
        // whether a superadmin exists yet (see
        // `bootstrap::stamp_admin_bounded_context`'s own doc comment). A
        // no-op on every startup but the process's genuine first one
        // against this database.
        //
        // Check-then-insert isn't atomic across two *separate* startups -
        // a real, ordinary scenario for this line specifically, not a
        // contrived one: several replicas of the same service, each
        // calling `.build()` against one shared database at deploy time,
        // can genuinely race here. If the insert fails, re-checking
        // rather than assuming failure is what tells "someone else's
        // concurrent build() already won this race" (benign - the row
        // exists either way, which is all this cares about) apart from a
        // real error (the row still doesn't exist, so whatever failed the
        // insert is a genuine problem worth propagating).
        let existing_admin_context = skilj_core::db::get_bounded_context(
            &pool,
            skilj_core::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME,
        )
        .await?;
        if let Some(admin_context) = skilj_core::bootstrap::stamp_admin_bounded_context(
            existing_admin_context.as_ref(),
            chrono::Utc::now(),
        ) {
            if let Err(e) = skilj_core::db::insert_bounded_context(&pool, &admin_context).await {
                if skilj_core::db::get_bounded_context(
                    &pool,
                    skilj_core::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME,
                )
                .await?
                .is_none()
                {
                    return Err(e);
                }
            }
        }

        // Needed unconditionally now, not only when a reconciliation role
        // is configured - `generate_bootstrap_secret` below needs the
        // same full-snapshot `existing_roles` `resolve_role_by_external_subject`
        // already did.
        let roles = skilj_core::db::list_roles(&pool).await?;

        let bootstrap_secret = skilj_core::bootstrap::generate_bootstrap_secret(&roles);
        if let Some(secret) = &bootstrap_secret {
            eprintln!(
                "skilj: no active superadmin Role exists yet - bootstrap secret (use once, via \
                 the createSuperadmin GraphQL mutation): {}",
                secret.secret
            );
        }

        // Taken here, ahead of the `Skilj` struct itself further down, so
        // the same `Arc`s (cheap to clone) can back both the
        // reconciliation dispatcher below - needed for
        // `needs_history_fold` (drift audit finding #3) - and the fields
        // `Skilj` is built with at the bottom of this function.
        // `command_types` is additionally needed to build the initial
        // `GraphqlState`/`SchemaRegistry` further down, before `Skilj`
        // itself exists to hand out `command_dispatcher()`.
        let projections = Arc::new(self.projections);
        let command_types = Arc::new(self.command_types);
        let snapshots = Arc::new(self.snapshots);

        // Codeberg issue #13: reconciliation immediately below never
        // looks up a templated tenant's own name (it only ever loops
        // over the literal keys this process's own `.bounded_context
        // (name)`/`#[auto_register]` calls declared, always a template's
        // name or an ordinary untemplated one - never a tenant's, chosen
        // later at runtime), so `reconciliation_dispatcher` just below
        // gets a real clone of this same cache for structural
        // consistency, even though every lookup it makes resolves to
        // itself.
        let bounded_contexts_for_warm_up = skilj_core::db::list_bounded_contexts(&pool).await?;
        let template_cache = skilj_core::template_cache::TemplateCache::new();
        template_cache.refresh(&pool).await?;

        let mut report = ReconciliationReport::default();
        if let Some(external_subject) = &self.reconciliation_role {
            let role = skilj_core::access_control::resolve_role_by_external_subject(
                external_subject,
                &roles,
            )?
            .clone();

            reconcile_event_types(
                &pool,
                &role,
                &self.event_types,
                &mut report,
                chrono::Utc::now(),
            )
            .await?;
            reconcile_command_types(&pool, &role, &command_types, &mut report).await?;
            let reconciliation_dispatcher = ProjectionDispatcherImpl {
                projections: projections.clone(),
                template_cache: template_cache.clone(),
            };
            reconcile_projections(
                &pool,
                &role,
                &projections,
                &mut report,
                &reconciliation_dispatcher,
            )
            .await?;
        }

        let identity_provider = self.identity_provider.map(|config| {
            Arc::new(IdentityProvider {
                cache: Arc::new(JwksCache::new(config.jwks_endpoint.clone())),
                config,
            })
        });

        let poll_interval = self.async_projection_poll_interval;
        let projection_query_wait_timeout = self.projection_query_wait_timeout;
        let encryption_master_key = self.encryption_master_key;
        let event_broadcaster = EventBroadcaster::new(self.event_broadcast_capacity);
        let revocation_broadcaster = RevocationBroadcaster::new();

        // The in-memory per-bounded-context event cache (drift audit
        // finding #8) - warmed here, before the first request can be
        // served, for every bounded context that exists yet (the same
        // `db::list_bounded_contexts` iteration the scheduler/async-
        // projection tasks below already use). A bounded context created
        // later starts its own window from empty on first touch -
        // `EventCache::append`/`try_events_after`'s own doc comments -
        // which is already correct, not a gap this loop needs to cover.
        //
        // Codeberg issue #15: run concurrently, `BACKGROUND_TASK_CONCURRENCY`
        // at a time, rather than one bounded context at a time - this used
        // to be a plain sequential `for` loop with `.await` inside, so
        // startup latency scaled linearly with bounded-context count.
        // Each iteration only ever touches its own bounded context's own
        // schema, so nothing here is shared mutable state across
        // iterations - safe to run out of order.
        let event_cache = EventCache::new(self.event_cache_warm_up_count);
        stream::iter(bounded_contexts_for_warm_up)
            .map(|bc| {
                let pool = &pool;
                let event_cache = &event_cache;
                async move {
                    event_cache.warm(pool, &bc.name).await?;
                    // Codeberg issue #12: `idempotency_keys` patched into
                    // every bounded context, every startup - including
                    // ones provisioned before this feature existed, since
                    // `provision_bounded_context_schema` itself only ever
                    // runs once, at creation, and there's no general
                    // per-bounded-context migration mechanism in this
                    // codebase. `CREATE TABLE IF NOT EXISTS` makes this
                    // free once the table already exists - see
                    // `ensure_idempotency_keys_table`'s own doc comment.
                    skilj_core::db::ensure_idempotency_keys_table(pool, &bc.name).await?;
                    // docs/architecture.md §37: patches an already-
                    // provisioned bounded context's `idempotency_keys`
                    // (a real table since 0.0.2) onto the `client_id`-
                    // scoped shape `ensure_idempotency_keys_table` above
                    // already gives a brand-new one directly - a no-op
                    // for one that already has it, migrated or fresh.
                    // See `migrate_idempotency_keys_client_id_scoping`'s
                    // own doc comment.
                    skilj_core::db::migrate_idempotency_keys_client_id_scoping(pool, &bc.name)
                        .await?;
                    // Cross-tenant projection read fix
                    // (docs/architecture.md's own write-up of this pass):
                    // same "patched into every bounded context, every
                    // startup" treatment, for `projection_state.owner`/
                    // `projection_rebuild_state.owner` instead - see
                    // `ensure_projection_state_owner_columns`'s own doc
                    // comment.
                    skilj_core::db::ensure_projection_state_owner_columns(pool, &bc.name).await?;
                    // Same pass's own raw-event half - `event_types.owner_tag_key`/
                    // `access_tokens.scope` - see
                    // `ensure_event_scoping_columns`'s own doc comment.
                    skilj_core::db::ensure_event_scoping_columns(pool, &bc.name).await?;
                    // Private-field mechanism (docs/architecture.md's own
                    // write-up of this pass) - `event_types.private_fields`/
                    // `command_types.private_fields` columns, and the new
                    // `private_field_grants` table - same "patched into
                    // every bounded context, every startup" treatment. See
                    // `ensure_private_field_columns`/
                    // `ensure_private_field_grants_table`'s own doc
                    // comments.
                    skilj_core::db::ensure_private_field_columns(pool, &bc.name).await?;
                    skilj_core::db::ensure_private_field_grants_table(pool, &bc.name).await?;
                    // Cross-context event router (docs/architecture.md's
                    // own write-up of this pass) - same "patched into
                    // every bounded context, every startup" treatment.
                    // See `ensure_cross_context_route_cursors_table`'s
                    // own doc comment.
                    skilj_core::db::ensure_cross_context_route_cursors_table(pool, &bc.name)
                        .await?;
                    // External-message dedup (docs/architecture.md §39,
                    // specs/skilj.allium's own rule CreateExternalEvent) -
                    // same "patched into every bounded context, every
                    // startup" treatment, for a brand-new table that needs
                    // no migration dance. See
                    // `ensure_external_message_cursors_table`'s own doc
                    // comment.
                    skilj_core::db::ensure_external_message_cursors_table(pool, &bc.name).await?;
                    // "New subscriber replays all history" fix
                    // (docs/architecture.md's own write-up of this pass) -
                    // `access_tokens.start_from`, same "patched into every
                    // bounded context, every startup" treatment. See
                    // `ensure_event_read_token_start_from_column`'s own
                    // doc comment.
                    skilj_core::db::ensure_event_read_token_start_from_column(pool, &bc.name)
                        .await?;
                    // Correlation/causation ids (Codeberg issue #18) -
                    // same "patched into every bounded context, every
                    // startup" treatment. See
                    // `ensure_correlation_causation_columns`'s own doc
                    // comment.
                    skilj_core::db::ensure_correlation_causation_columns(pool, &bc.name).await?;
                    Ok::<(), skilj_core::Error>(())
                }
            })
            .buffer_unordered(BACKGROUND_TASK_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await?;

        // The initial GraphQL schema (Codeberg issue #2; `@guarantee
        // RegistrationReachesEveryInstance`) - built here, ahead of
        // `Skilj` itself, the same "can't call `self.graphql_state()`
        // before `self` exists" reasoning `reconciliation_dispatcher`
        // above already works around, applied to the whole `GraphqlState`
        // this time rather than just one dispatcher.
        let schema_registry = Arc::new(
            skilj_graphql::schema::SchemaRegistry::build(skilj_graphql::GraphqlState {
                pool: pool.clone(),
                bootstrap_secret: bootstrap_secret.clone(),
                identity: identity_provider
                    .as_ref()
                    .map(|ip| skilj_graphql::auth::Identity {
                        config: ip.config.clone(),
                        cache: Arc::clone(&ip.cache),
                    }),
                dispatcher: Arc::new(Dispatcher {
                    command_types: command_types.clone(),
                    template_cache: template_cache.clone(),
                }),
                projection_dispatcher: Arc::new(ProjectionDispatcherImpl {
                    projections: projections.clone(),
                    template_cache: template_cache.clone(),
                }),
                snapshot_dispatcher: Arc::new(SnapshotDispatcherImpl {
                    snapshots: snapshots.clone(),
                    template_cache: template_cache.clone(),
                }),
                projection_query_wait_timeout,
                encryption_master_key: encryption_master_key.clone(),
                event_broadcaster: event_broadcaster.clone(),
                revocation_broadcaster: revocation_broadcaster.clone(),
                event_cache: event_cache.clone(),
                template_cache: template_cache.clone(),
            })
            .await?,
        );

        let skilj = Skilj {
            pool,
            command_types,
            projections,
            snapshots,
            event_types: Arc::new(self.event_types),
            bootstrap_secret,
            identity_provider,
            projection_query_wait_timeout,
            encryption_master_key,
            event_broadcaster,
            revocation_broadcaster,
            event_cache,
            schema_registry,
            template_cache,
        };

        // The single shared background task backing §8 item 6's async
        // case - one task, not one per bounded context, since a bounded
        // context can gain its first async Projection/ProjectionRebuild
        // at any point after this returns (via GraphQL `registerProjection`),
        // not only at build time. Detached, no shutdown API this pass -
        // it runs for the process's lifetime, the same "don't build ahead
        // of what's wired" call this crate has made before (nothing today
        // needs to gracefully stop a running `Skilj`). Runs its first
        // catch-up immediately, before the first sleep, so a caller
        // creating an event right after `.build()` returns doesn't also
        // pay for a full idle poll interval on top of processing time.
        let poll_pool = skilj.pool.clone();
        let poll_dispatcher = skilj.projection_dispatcher();
        tokio::spawn(async move {
            loop {
                let start = std::time::Instant::now();
                // One span per tick, a trace root - there's no HTTP
                // request for this to inherit a parent from.
                async {
                    match skilj_core::db::list_bounded_contexts(&poll_pool).await {
                        Ok(bounded_contexts) => {
                            // Codeberg issue #15: concurrent, not one bc
                            // at a time - each bc's own catch-up only
                            // ever touches its own schema, so nothing
                            // here is shared mutable state across
                            // iterations.
                            stream::iter(bounded_contexts)
                                .for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, |bc| {
                                    let poll_pool = poll_pool.clone();
                                    let poll_dispatcher = poll_dispatcher.clone();
                                    async move {
                                        if let Err(e) = skilj_core::db::catch_up_bounded_context(
                                            &poll_pool,
                                            &bc.name,
                                            poll_dispatcher.as_ref(),
                                        )
                                        .await
                                        {
                                            tracing::warn!(
                                                bounded_context = %bc.name,
                                                error = %e,
                                                "async projection catch-up failed"
                                            );
                                            BACKGROUND_TASK_ERRORS.add(
                                                1,
                                                &[
                                                    KeyValue::new("task", "async_projection"),
                                                    KeyValue::new("reason", "catch_up_failed"),
                                                ],
                                            );
                                        }
                                    }
                                })
                                .await;
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "async projection catch-up failed to list bounded contexts"
                            );
                            BACKGROUND_TASK_ERRORS.add(
                                1,
                                &[
                                    KeyValue::new("task", "async_projection"),
                                    KeyValue::new("reason", "list_bounded_contexts_failed"),
                                ],
                            );
                        }
                    }
                }
                .instrument(tracing::info_span!("async_projection_tick"))
                .await;
                BACKGROUND_TASK_TICK_DURATION.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("task", "async_projection")],
                );
                tokio::time::sleep(poll_interval).await;
            }
        });

        // docs/architecture.md §19's own background task - one shared
        // task, not one per bounded context, for the identical reasons
        // the async projection task above is. Detached, runs for the
        // process's lifetime, same as every other background task here.
        // Deliberately its own task, not folded into the async
        // projection one above even though the shape rhymes closely -
        // see `skilj_core::plugin::Snapshot`'s own doc comment for why
        // `Snapshot` stays structurally separate from `Projection`
        // throughout.
        let snapshot_pool = skilj.pool.clone();
        let snapshot_dispatcher = skilj.snapshot_dispatcher();
        let snapshot_interval = self.snapshot_poll_interval;
        tokio::spawn(async move {
            loop {
                let start = std::time::Instant::now();
                async {
                    match skilj_core::db::list_bounded_contexts(&snapshot_pool).await {
                        Ok(bounded_contexts) => {
                            // Codeberg issue #15: concurrent, same
                            // reasoning as the async-projection task
                            // above.
                            stream::iter(bounded_contexts)
                                .for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, |bc| {
                                    let snapshot_pool = snapshot_pool.clone();
                                    let snapshot_dispatcher = snapshot_dispatcher.clone();
                                    async move {
                                        if let Err(e) = skilj_core::db::catch_up_snapshots(
                                            &snapshot_pool,
                                            &bc.name,
                                            snapshot_dispatcher.as_ref(),
                                        )
                                        .await
                                        {
                                            tracing::warn!(
                                                bounded_context = %bc.name,
                                                error = %e,
                                                "snapshot catch-up failed"
                                            );
                                            BACKGROUND_TASK_ERRORS.add(
                                                1,
                                                &[
                                                    KeyValue::new("task", "snapshot"),
                                                    KeyValue::new("reason", "catch_up_failed"),
                                                ],
                                            );
                                        }
                                    }
                                })
                                .await;
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "snapshot catch-up failed to list bounded contexts"
                            );
                            BACKGROUND_TASK_ERRORS.add(
                                1,
                                &[
                                    KeyValue::new("task", "snapshot"),
                                    KeyValue::new("reason", "list_bounded_contexts_failed"),
                                ],
                            );
                        }
                    }
                }
                .instrument(tracing::info_span!("snapshot_tick"))
                .await;
                BACKGROUND_TASK_TICK_DURATION.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("task", "snapshot")],
                );
                tokio::time::sleep(snapshot_interval).await;
            }
        });

        // The background task driving `CrossContextRoute`s - this
        // crate's own answer to "make messages cross bounded contexts
        // without needing an external system like Temporal" (see
        // `skilj_core::plugin::CrossContextRoute`'s own doc comment for
        // why this stays a single-hop reaction, not a Saga/process
        // manager). One shared task, not one per route, for the same
        // reasons the async projection task above is; detached, runs
        // for the process's lifetime, same as every other background
        // task here. The route list itself is fixed at `.build()` time
        // (registered via `SkiljBuilder::cross_context_route`, no
        // runtime registration surface, matching every other plugin
        // trait), so it's read once here rather than re-listed every
        // tick the way bounded contexts are.
        let route_pool = skilj.pool.clone();
        let route_command_dispatcher = skilj.command_dispatcher();
        let route_projection_dispatcher = skilj.projection_dispatcher();
        let route_snapshot_dispatcher = skilj.snapshot_dispatcher();
        let route_broadcaster = skilj.event_broadcaster.clone();
        let route_event_cache = skilj.event_cache.clone();
        let route_encryption_master_key = skilj.encryption_master_key.clone();
        let route_dispatcher: Arc<dyn skilj_core::plugin::CrossContextRouteDispatcher> =
            Arc::new(CrossContextRouteDispatcherImpl {
                routes: Arc::new(self.cross_context_routes),
            });
        let routes = route_dispatcher.routes();
        let route_interval = self.cross_context_route_poll_interval;
        tokio::spawn(async move {
            loop {
                let start = std::time::Instant::now();
                async {
                    stream::iter(routes.clone())
                        .for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, |route| {
                            let route_pool = route_pool.clone();
                            let route_dispatcher = route_dispatcher.clone();
                            let route_command_dispatcher = route_command_dispatcher.clone();
                            let route_projection_dispatcher = route_projection_dispatcher.clone();
                            let route_snapshot_dispatcher = route_snapshot_dispatcher.clone();
                            let route_broadcaster = route_broadcaster.clone();
                            let route_event_cache = route_event_cache.clone();
                            let route_encryption_master_key = route_encryption_master_key.clone();
                            async move {
                                if let Err(e) = skilj_core::db::catch_up_cross_context_route(
                                    &route_pool,
                                    &route,
                                    route_dispatcher.as_ref(),
                                    route_command_dispatcher.as_ref(),
                                    route_projection_dispatcher.as_ref(),
                                    route_snapshot_dispatcher.as_ref(),
                                    &route_broadcaster,
                                    &route_event_cache,
                                    route_encryption_master_key.as_ref(),
                                )
                                .await
                                {
                                    tracing::warn!(
                                        route = %route.name,
                                        error = %e,
                                        "cross-context route catch-up failed"
                                    );
                                    BACKGROUND_TASK_ERRORS.add(
                                        1,
                                        &[
                                            KeyValue::new("task", "cross_context_route"),
                                            KeyValue::new("reason", "catch_up_failed"),
                                        ],
                                    );
                                }
                            }
                        })
                        .await;
                }
                .instrument(tracing::info_span!("cross_context_route_tick"))
                .await;
                BACKGROUND_TASK_TICK_DURATION.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("task", "cross_context_route")],
                );
                tokio::time::sleep(route_interval).await;
            }
        });

        // The background scheduler backing `rule CreateSystemEvent`/
        // `rule SkipMissedOccurrences` - one shared task, not one per
        // bounded context or event type, for the same reasons the async
        // projection task above is; detached, runs for the process's
        // lifetime, same as that task too. Its very first tick, run
        // immediately rather than after the first sleep (identical
        // reasoning to the poll task above), already covers what a
        // separate "startup catch-up pass" would: `scheduler_tick`'s own
        // backlog check (see its doc comment) runs on every tick, first
        // one included, so a `skip`-policy type this process finds
        // already behind on its very first look is caught up right then,
        // not held for a second tick.
        let scheduler_pool = skilj.pool.clone();
        let scheduler_projection_dispatcher = skilj.projection_dispatcher();
        let scheduler_event_dispatcher = skilj.event_dispatcher();
        let scheduler_broadcaster = skilj.event_broadcaster.clone();
        let scheduler_event_cache = skilj.event_cache.clone();
        let scheduler_encryption_master_key = skilj.encryption_master_key.clone();
        let scheduler_interval = self.scheduler_poll_interval;
        tokio::spawn(async move {
            loop {
                let start = std::time::Instant::now();
                scheduler_tick(
                    &scheduler_pool,
                    scheduler_projection_dispatcher.as_ref(),
                    scheduler_event_dispatcher.as_ref(),
                    &scheduler_broadcaster,
                    &scheduler_event_cache,
                    scheduler_encryption_master_key.as_ref(),
                    chrono::Utc::now(),
                )
                // One span per tick, a trace root - same reasoning as
                // the async projection task above.
                .instrument(tracing::info_span!("scheduler_tick"))
                .await;
                BACKGROUND_TASK_TICK_DURATION.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("task", "scheduler")],
                );
                tokio::time::sleep(scheduler_interval).await;
            }
        });

        // Cross-instance push completeness (Codeberg issue #2;
        // `@guarantee DeliverySpansInstances`/`RegistrationReachesEveryInstance`
        // in specs/skilj.allium) - one more shared background task,
        // spawned the same "one task, detached, runs for the process's
        // lifetime" way as the two above. Connects its own dedicated
        // `PgListener` (see `skilj_core::cross_instance::Listener`'s own
        // module doc comment for the full design) and, for each message
        // received, dispatches into the one local destination that
        // message is really about: an `EventAppended` pointer is
        // refetched and republished into this instance's own
        // `EventBroadcaster` - `EventSubscription`'s live subscribers
        // here don't distinguish an event committed on this instance
        // from one committed on another, they're already reading through
        // the same broadcaster either way. `Revoked` republishes into
        // `RevocationBroadcaster` the identical way. `RegistrationChanged`
        // rebuilds and swaps `schema_registry`, and (Codeberg issue #13)
        // refreshes `template_cache` from the same fresh
        // `list_bounded_contexts` read - `insert_bounded_context` already
        // fires this exact notification for every new context, templated
        // or not, so a new tenant's own dispatch resolution becomes live
        // on every instance without any dedicated plumbing of its own.
        //
        // `Listener::connect` itself is retried in a loop (unlike
        // `recv()`, which `sqlx::postgres::PgListener` already retries
        // internally - see that type's own doc comment) - otherwise a
        // Postgres outage right at startup would leave this instance
        // permanently deaf to every other instance's writes for the rest
        // of its life, rather than just until the database comes back.
        let cross_instance_pool = skilj.pool.clone();
        let cross_instance_event_broadcaster = skilj.event_broadcaster.clone();
        let cross_instance_revocation_broadcaster = skilj.revocation_broadcaster.clone();
        let cross_instance_schema_registry = Arc::clone(&skilj.schema_registry);
        let cross_instance_template_cache = skilj.template_cache.clone();
        let cross_instance_state = skilj.graphql_state();
        tokio::spawn(async move {
            let mut listener = loop {
                match skilj_core::cross_instance::Listener::connect(&cross_instance_pool).await {
                    Ok(listener) => break listener,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "cross-instance listener failed to connect, retrying"
                        );
                        BACKGROUND_TASK_ERRORS.add(
                            1,
                            &[
                                KeyValue::new("task", "cross_instance"),
                                KeyValue::new("reason", "connect_failed"),
                            ],
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            };
            loop {
                let message = match listener
                    .recv()
                    .instrument(tracing::info_span!("cross_instance_recv"))
                    .await
                {
                    Ok(message) => message,
                    Err(err) => {
                        tracing::warn!(error = %err, "cross-instance listener error, retrying");
                        BACKGROUND_TASK_ERRORS.add(
                            1,
                            &[
                                KeyValue::new("task", "cross_instance"),
                                KeyValue::new("reason", "recv_failed"),
                            ],
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };
                match message {
                    skilj_core::cross_instance::Message::EventAppended {
                        bounded_context,
                        sequence,
                        origin_instance_id,
                    } => {
                        // Self-NOTIFY dedup (Codeberg ultra-review
                        // bug_001): Postgres delivers a NOTIFY to every
                        // listening backend, including ones opened by the
                        // same process that sent it, so this instance's
                        // own writes echo back here too. The write path
                        // already called `cross_instance_event_broadcaster
                        // .publish(&event)` directly at commit time
                        // (`db::submit_command` and friends) - refetching
                        // and republishing that identical event into that
                        // identical broadcaster again would deliver it
                        // twice to every local `EventSubscription`
                        // subscriber. See `EventBroadcaster::instance_id`'s
                        // own doc comment for the full explanation.
                        if origin_instance_id == cross_instance_event_broadcaster.instance_id() {
                            continue;
                        }
                        // The plain, uncached read - deliberately, not
                        // `get_event_by_sequence_cached`. A one-shot read
                        // triggered by a notification has no repeated-read
                        // benefit to gain from the cache, so there's
                        // nothing this trades away; going through the
                        // cache here would also risk double-appending an
                        // event `EventCache::try_event_by_sequence`'s own
                        // `freshen()` backfill already has no dedup
                        // against.
                        match skilj_core::db::get_event_by_sequence(
                            &cross_instance_pool,
                            &bounded_context,
                            sequence,
                        )
                        .await
                        {
                            Ok(Some(event)) => cross_instance_event_broadcaster.publish(&event),
                            // Already gone by the time this instance
                            // looked - a deleted bounded context, most
                            // plausibly. Nothing to deliver, not an
                            // error.
                            Ok(None) => {}
                            Err(err) => tracing::warn!(
                                error = %err,
                                bounded_context = %bounded_context,
                                sequence,
                                "cross-instance event refetch failed"
                            ),
                        }
                    }
                    skilj_core::cross_instance::Message::Revoked {
                        revoked,
                        origin_instance_id,
                    } => {
                        // Same self-NOTIFY dedup as EventAppended above,
                        // against RevocationBroadcaster::instance_id
                        // instead - the write path already published
                        // this revocation into
                        // cross_instance_revocation_broadcaster directly.
                        if origin_instance_id != cross_instance_revocation_broadcaster.instance_id()
                        {
                            cross_instance_revocation_broadcaster.publish(revoked);
                        }
                    }
                    skilj_core::cross_instance::Message::RegistrationChanged => {
                        if let Err(err) = cross_instance_schema_registry
                            .rebuild(cross_instance_state.clone())
                            .await
                        {
                            tracing::warn!(error = %err, "cross-instance schema rebuild failed");
                        }
                        // Codeberg issue #13 - see the note above this
                        // task's own spawn for why this shares the same
                        // notification `schema_registry.rebuild` does.
                        if let Err(err) = cross_instance_template_cache
                            .refresh(&cross_instance_pool)
                            .await
                        {
                            tracing::warn!(
                                error = %err,
                                "cross-instance template cache refresh failed"
                            );
                        }
                    }
                }
            }
        });

        Ok((skilj, report))
    }
}

/// A generous bound on how many occurrences one event type's own backlog
/// gets raised in a single `scheduler_tick`, not a correctness
/// requirement - without one, an event type owed a truly enormous
/// `replay_backlog` after a long outage could block this tick, shared by
/// every other bounded context and event type, for as long as fully
/// catching it up takes. Occurrences beyond this cap are simply left for
/// the next tick, which resumes from wherever the real, shared position
/// actually landed - the same "recent window, not a hard limit on
/// correctness" register `EventCache`'s own `capacity` is in, which is
/// also where this exact number comes from.
const MAX_OCCURRENCES_PER_TICK: usize = 1000;

/// One scheduler tick, across every active bounded context - `rule
/// CreateSystemEvent`/`rule SkipMissedOccurrences`'s own wake mechanism,
/// factored out of `SkiljBuilder::build`'s spawned task so its control
/// flow (nested loops, several early-`continue`s) doesn't have to live
/// inside a `tokio::spawn` closure.
///
/// For each `system_triggered_allowed` `EventType` `db::
/// list_scheduled_event_types` returns, raises every occurrence its own
/// `schedule_position` hasn't accounted for yet that is already due, in
/// ascending order, up to `MAX_OCCURRENCES_PER_TICK` - the spec's own
/// "the scheduler's own side of that contract is uniform across all
/// three policies... raise every occurrence beyond the position that has
/// come due, in ascending order, and nothing else" (the note above
/// `rule CreateSystemEvent`), not just the single earliest one. A local,
/// tick-scoped `cursor` walks this search forward - `next_occurrence_after`
/// would otherwise recompute the identical earliest occurrence forever,
/// since only a *successful* fire ever advances the real, shared
/// position - without writing anywhere itself; only `db::fire_system_event`/
/// `skip_missed_occurrences_for_event_type` ever touch the real
/// `schedule_position`. This is what lets `fire_once` genuinely reach a
/// backlog's own last occurrence within one tick, rather than being
/// offered the same, correctly-rejected earliest one on every future
/// tick too (the earlier version of this function's own bug: it only
/// ever raised the first occurrence, so a `fire_once` type more than one
/// occurrence behind could never fire again).
///
/// Each raised occurrence is handled per policy:
///
///   - under `skip`, first checks whether a *second* occurrence is also
///     already due (`nothing_later_is_due` - the identical computation
///     `event_store::create_system_event`'s own eligibility gate makes
///     internally) - a real backlog, which `skip` can only ever resolve
///     by jumping straight to `now` (`db::skip_missed_occurrences_for_event_type`),
///     never by raising occurrence by occurrence (every one but the
///     last would just be correctly rejected for a gap anyway, per
///     `no_gap`) - and stops raising further occurrences for this event
///     type this tick either way, the same "closed without firing"
///     `SkipMissedOccurrences` itself is;
///   - otherwise, attempts to fire it via `db::fire_system_event`, which
///     re-derives eligibility for real under its own row lock - this
///     function's own checks are a cheap, non-authoritative pre-filter
///     only, the same "peek here, re-check under lock there" split
///     `submit_command` already uses. `replay_backlog` fires every
///     occurrence in turn; `fire_once` correctly rejects every one but
///     the backlog's own last, which the `cursor` walking forward is
///     what actually lets it reach.
///
/// A database error at any step is logged and that event type's own
/// remaining backlog abandoned for this tick (not the whole function -
/// other bounded contexts/event types still get their own turn), the
/// same tolerance `catch_up_bounded_context`'s own poll task already has
/// for its own errors; the next tick tries again from wherever the real
/// position landed.
async fn scheduler_tick(
    pool: &Pool,
    projection_dispatcher: &dyn skilj_core::plugin::ProjectionDispatcher,
    event_dispatcher: &dyn skilj_core::plugin::EventDispatcher,
    broadcaster: &EventBroadcaster,
    event_cache: &EventCache,
    encryption_master_key: Option<&EncryptionMasterKey>,
    now: chrono::DateTime<chrono::Utc>,
) {
    let bounded_contexts = match skilj_core::db::list_bounded_contexts(pool).await {
        Ok(bcs) => bcs,
        Err(e) => {
            tracing::warn!(error = %e, "scheduler failed to list bounded contexts");
            BACKGROUND_TASK_ERRORS.add(
                1,
                &[
                    KeyValue::new("task", "scheduler"),
                    KeyValue::new("reason", "list_bounded_contexts_failed"),
                ],
            );
            return;
        }
    };
    // Codeberg issue #15: concurrent, same reasoning as the async-
    // projection/snapshot tasks - each bounded context's own schedule
    // check only ever touches its own schema. The per-event-type/
    // per-occurrence walk *within* one bounded context stays exactly as
    // sequential as it already was (`scheduler_tick_for_bounded_context`
    // below is an unmodified extraction of what this loop's body already
    // did, not a behaviour change).
    stream::iter(&bounded_contexts)
        .for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, |bc| {
            scheduler_tick_for_bounded_context(
                pool,
                projection_dispatcher,
                event_dispatcher,
                broadcaster,
                event_cache,
                encryption_master_key,
                now,
                bc,
            )
        })
        .await;
}

/// One bounded context's own share of `scheduler_tick` - an unmodified
/// extraction of what used to be that function's own loop body, so
/// Codeberg issue #15's `for_each_concurrent` fan-out has something to
/// call per bounded context.
#[allow(clippy::too_many_arguments)]
async fn scheduler_tick_for_bounded_context(
    pool: &Pool,
    projection_dispatcher: &dyn skilj_core::plugin::ProjectionDispatcher,
    event_dispatcher: &dyn skilj_core::plugin::EventDispatcher,
    broadcaster: &EventBroadcaster,
    event_cache: &EventCache,
    encryption_master_key: Option<&EncryptionMasterKey>,
    now: chrono::DateTime<chrono::Utc>,
    bc: &skilj_core::event_store::BoundedContext,
) {
    if bc.status != skilj_core::event_store::BoundedContextStatus::Active {
        return;
    }
    let scheduled = match skilj_core::db::list_scheduled_event_types(pool, &bc.name).await {
        Ok(scheduled) => scheduled,
        Err(e) => {
            tracing::warn!(
                bounded_context = %bc.name,
                error = %e,
                "scheduler failed to list scheduled event types"
            );
            BACKGROUND_TASK_ERRORS.add(
                1,
                &[
                    KeyValue::new("task", "scheduler"),
                    KeyValue::new("reason", "list_scheduled_event_types_failed"),
                ],
            );
            return;
        }
    };
    for et in &scheduled {
        let (Some(schedule), Some(policy), Some(initial_position)) = (
            &et.system_triggered_schedule,
            et.missed_occurrence_policy,
            et.schedule_position,
        ) else {
            continue;
        };

        let mut cursor = initial_position;
        for _ in 0..MAX_OCCURRENCES_PER_TICK {
            let Some(occurrence_at) =
                skilj_core::event_store::next_occurrence_after(schedule, cursor)
            else {
                break;
            };
            if occurrence_at > now {
                break;
            }

            let nothing_later_is_due =
                skilj_core::event_store::next_occurrence_after(schedule, occurrence_at)
                    .is_none_or(|next| next > now);

            if policy == skilj_core::event_store::MissedOccurrencePolicy::Skip
                && !nothing_later_is_due
            {
                if let Err(e) = skilj_core::db::skip_missed_occurrences_for_event_type(
                    pool, &bc.name, &et.name, now,
                )
                .await
                {
                    tracing::warn!(
                        bounded_context = %bc.name,
                        event_type = %et.name,
                        error = %e,
                        "SkipMissedOccurrences failed"
                    );
                    BACKGROUND_TASK_ERRORS.add(
                        1,
                        &[
                            KeyValue::new("task", "scheduler"),
                            KeyValue::new("reason", "skip_missed_occurrences_failed"),
                        ],
                    );
                }
                break;
            }

            if let Err(e) = skilj_core::db::fire_system_event(
                pool,
                projection_dispatcher,
                event_dispatcher,
                broadcaster,
                event_cache,
                &bc.name,
                &et.name,
                occurrence_at,
                now,
                encryption_master_key,
            )
            .await
            {
                tracing::warn!(
                    bounded_context = %bc.name,
                    event_type = %et.name,
                    error = %e,
                    "CreateSystemEvent failed"
                );
                BACKGROUND_TASK_ERRORS.add(
                    1,
                    &[
                        KeyValue::new("task", "scheduler"),
                        KeyValue::new("reason", "create_system_event_failed"),
                    ],
                );
                break;
            }

            // Whether this occurrence actually produced an event
            // (`fire_once` rejects every one but a backlog's own
            // last) or not, this tick's own search has to move past
            // it - see this function's own doc comment for why.
            cursor = occurrence_at;
        }
    }
}

/// `Some(mapping)` only when it's both active *and* admin-level - the
/// exact pair `register_event_type`/`register_command_type`/
/// `register_projection` each require internally (they'd otherwise
/// reject with `InsufficientAccessLevel`, an `Err` that would fail
/// `.build()` outright). Reconciliation instead treats "no admin access
/// yet" as a skip, per §1.5 - so this check has to happen *before*
/// calling into the rule, not be left to the rule's own rejection.
async fn active_admin_mapping(
    pool: &Pool,
    role_id: &str,
    bounded_context: &str,
) -> skilj_core::error::Result<Option<skilj_core::access_control::RoleAccessMapping>> {
    Ok(
        skilj_core::db::get_active_role_access_mapping(pool, role_id, bounded_context)
            .await?
            .filter(|m| m.level == AccessLevel::Admin),
    )
}

/// The first two steps every `reconcile_*` function below shares, byte
/// for byte - resolve the target `BoundedContext` and the reconciliation
/// role's own admin mapping on it, `None` (a skip, not an error) if
/// either is missing. Real duplication, but a plain function extraction,
/// not a generic one: each `reconcile_*` function's own register/persist
/// steps diverge too much (different `register_*` pure function
/// signatures, different persistence shapes) for a shared generic to
/// actually save more than this shared prefix already does.
async fn resolve_bounded_context_and_admin_mapping(
    pool: &Pool,
    role_id: &str,
    bounded_context_name: &str,
) -> skilj_core::error::Result<
    Option<(
        skilj_core::event_store::BoundedContext,
        skilj_core::access_control::RoleAccessMapping,
    )>,
> {
    let Some(bc) = skilj_core::db::get_bounded_context(pool, bounded_context_name).await? else {
        return Ok(None);
    };
    let Some(mapping) = active_admin_mapping(pool, role_id, bounded_context_name).await? else {
        return Ok(None);
    };
    Ok(Some((bc, mapping)))
}

async fn reconcile_event_types(
    pool: &Pool,
    role: &Role,
    event_types: &HashMap<(String, String), RegisteredEventType>,
    report: &mut ReconciliationReport,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), skilj_core::Error> {
    for ((bounded_context_name, name), registered) in event_types {
        let key = format!("{bounded_context_name}/{name}");
        let Some((bc, mapping)) =
            resolve_bounded_context_and_admin_mapping(pool, &role.id, bounded_context_name).await?
        else {
            report.skipped_no_access.push(key);
            continue;
        };

        let existing = skilj_core::db::get_event_type(pool, bounded_context_name, name).await?;
        let registration = skilj_core::event_store::register_event_type(
            &mapping,
            &bc,
            name.clone(),
            registered.schema.clone(),
            registered.tag_mappings.clone(),
            registered.owner_tag_key.clone(),
            registered.sensitive_fields.clone(),
            registered.private_fields.clone(),
            registered.external_creation_allowed,
            registered.direct_creation_allowed,
            registered.system_triggered_allowed,
            registered.system_triggered_schedule.clone(),
            registered.missed_occurrence_policy,
            registered.event_read_allowed,
            existing.as_ref(),
            now,
        )?;
        skilj_core::db::upsert_event_type(pool, registration.event_type()).await?;
        report.registered.push(key);
    }
    Ok(())
}

async fn reconcile_command_types(
    pool: &Pool,
    role: &Role,
    command_types: &HashMap<(String, String), RegisteredCommandType>,
    report: &mut ReconciliationReport,
) -> Result<(), skilj_core::Error> {
    for ((bounded_context_name, name), registered) in command_types {
        let key = format!("{bounded_context_name}/{name}");
        let Some((bc, mapping)) =
            resolve_bounded_context_and_admin_mapping(pool, &role.id, bounded_context_name).await?
        else {
            report.skipped_no_access.push(key);
            continue;
        };

        let existing = skilj_core::db::get_command_type(pool, bounded_context_name, name).await?;
        let registration = skilj_core::event_store::register_command_type(
            &mapping,
            &bc,
            name.clone(),
            registered.schema.clone(),
            registered.tag_mappings.clone(),
            registered.owner_tag_key.clone(),
            registered.sensitive_fields.clone(),
            registered.private_fields.clone(),
            registered.rest_trigger_allowed,
            existing.as_ref(),
        )?;
        skilj_core::db::upsert_command_type(pool, registration.command_type()).await?;
        report.registered.push(key);
    }
    Ok(())
}

/// `None` when any named `EventType` isn't registered in this bounded
/// context yet - reconciliation treats that the same as "no admin access
/// yet" (a skip, not an error): a `Projection`'s own `EventType`
/// registrations may simply not have reconciled yet, in the same
/// registration pass or a not-yet-run one, and `RegisterProjection`
/// itself has no other way to express "wait and retry" for this case.
async fn resolve_consumed_event_types(
    pool: &Pool,
    bounded_context: &str,
    names: &[&'static str],
) -> skilj_core::error::Result<Option<Vec<skilj_core::event_store::EventType>>> {
    let mut event_types = Vec::with_capacity(names.len());
    for name in names {
        let Some(et) = skilj_core::db::get_event_type(pool, bounded_context, name).await? else {
            return Ok(None);
        };
        event_types.push(et);
    }
    Ok(Some(event_types))
}

async fn reconcile_projections(
    pool: &Pool,
    role: &Role,
    projections: &HashMap<(String, String), RegisteredProjection>,
    report: &mut ReconciliationReport,
    dispatcher: &dyn skilj_core::plugin::ProjectionDispatcher,
) -> Result<(), skilj_core::Error> {
    for ((bounded_context_name, name), registered) in projections {
        let key = format!("{bounded_context_name}/{name}");
        let Some((bc, mapping)) =
            resolve_bounded_context_and_admin_mapping(pool, &role.id, bounded_context_name).await?
        else {
            report.skipped_no_access.push(key);
            continue;
        };
        let Some(consumed_event_types) = resolve_consumed_event_types(
            pool,
            bounded_context_name,
            &registered.consumed_event_types,
        )
        .await?
        else {
            report.skipped_no_access.push(key);
            continue;
        };

        let existing = skilj_core::db::get_projection(pool, bounded_context_name, name).await?;
        // RegisterProjection's own `staged` is always the pending row -
        // see `skilj-graphql`'s `resolvers::type_registration`'s own
        // module doc comment for why every lookup here is explicit about
        // it now.
        let staged = skilj_core::db::get_projection_rebuild(
            pool,
            bounded_context_name,
            name,
            ProjectionRebuildStatus::Pending,
        )
        .await?;
        let bounded_context_events =
            skilj_core::db::list_events_for_bounded_context(pool, bounded_context_name).await?;

        let registration = skilj_core::projections::register_projection(
            &mapping,
            &bc,
            name.clone(),
            registered.schema.clone(),
            consumed_event_types,
            registered.sync,
            existing.as_ref(),
            staged.as_ref(),
            &bounded_context_events,
        )?;
        match registration {
            ProjectionRegistration::Created {
                projection,
                needs_history_fold,
            } => {
                skilj_core::db::upsert_projection(pool, &projection).await?;
                // No `projection_state` seeding here anymore (§9's
                // "keyed / multi-row Projections" pass) - a projection's
                // own instances aren't known until events actually name
                // them, so every instance's own row is created lazily,
                // on first touch, the same uniform path whether this
                // projection ever uses a real key or stays on the
                // implicit single one. `needs_history_fold` (drift audit
                // finding #3) is the one exception: a first-time sync
                // projection with pre-existing matching history is
                // folded immediately, right here, rather than waiting on
                // any lazy touch that will never come for events already
                // committed before this registration.
                if needs_history_fold {
                    skilj_core::db::fold_history_into_new_sync_projection(
                        pool,
                        &projection,
                        dispatcher,
                    )
                    .await?;
                }
            }
            ProjectionRegistration::ReconciledTrivially(projection) => {
                skilj_core::db::upsert_projection(pool, &projection).await?;
            }
            ProjectionRegistration::RebuildStaged(rebuild) => {
                skilj_core::db::upsert_projection_rebuild(pool, &rebuild).await?;
            }
        }
        report.registered.push(key);
    }
    Ok(())
}
