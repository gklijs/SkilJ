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

/// [`SkiljBuilder::idempotency_key_retention`]'s default: one hour.
pub const DEFAULT_IDEMPOTENCY_KEY_RETENTION: std::time::Duration =
    std::time::Duration::from_secs(60 * 60);

/// How often the idempotency-key retention task runs at most; a key
/// outlives its retention by at most about this much. A shorter
/// retention runs it that often instead (but not more than once a
/// second).
const IDEMPOTENCY_KEY_CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// How long an instance waits after startup before its first retention
/// sweep, at most - `min(retention, this)` (docs/architecture.md §94).
const IDEMPOTENCY_KEY_STARTUP_GRACE: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Rows one retention `DELETE` removes at most; the task repeats until a
/// batch comes back short.
const IDEMPOTENCY_KEY_CLEANUP_BATCH: i64 = 10_000;

/// [`SkiljBuilder::deadline_retention`]'s default: 30 days.
pub const DEFAULT_DEADLINE_RETENTION: std::time::Duration =
    std::time::Duration::from_secs(30 * 24 * 60 * 60);

/// How often the deadline retention task runs at most (docs/architecture.md
/// §163). A resolved deadline outlives its retention by at most about this
/// much.
const DEADLINE_CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Runs one unit of a background task's work (one bounded context's, one
/// route's, one deadline schedule's), turning a panic into a logged error
/// and a `reason = "panicked"` error count. The units run application
/// plugin code (`Projection::project`, `Snapshot`s, routes, deadlines,
/// deciders), and each task is a single detached `tokio::spawn`: an
/// uncontained panic unwound out of `for_each_concurrent` and ended the
/// task for every bounded context, for the rest of the process's life,
/// with nothing but tokio's own panic message to show for it
/// (docs/architecture.md §80). Whatever the unit had open rolls back when
/// its transaction is dropped during the unwind, so the next tick retries
/// it, like any other failed unit.
async fn contain_panic(
    task: &'static str,
    unit: String,
    work: impl std::future::Future<Output = ()>,
) {
    use futures_util::FutureExt;
    if let Err(panic) = std::panic::AssertUnwindSafe(work).catch_unwind().await {
        let message = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string panic payload".to_string());
        tracing::error!(task, unit = %unit, panic = %message, "background task unit panicked");
        BACKGROUND_TASK_ERRORS.add(
            1,
            &[
                KeyValue::new("task", task),
                KeyValue::new("reason", "panicked"),
            ],
        );
    }
}

/// Re-exported so a crate using `#[auto_register]` (whose expansion emits
/// `::skilj::inventory::submit! { ... }`) needs only its existing `skilj`
/// dependency - not a direct one on `inventory` too. Not meant to be used
/// directly by hand-written code; `SkiljBuilder::auto_register()` is the
/// intended entry point.
pub use inventory;
pub use skilj_core::access_control::{IdpConfig, SigningAlgorithm};
pub use skilj_core::encryption::EncryptionMasterKey;
pub use skilj_core::plugin::{
    requires_role, CancelDeadline, CommandType, CrossContextRoute, EventType, Projection,
    ScheduleDeadline, Snapshot, DEFAULT_BOUNDED_CONTEXT,
};
pub use skilj_graphql::limits::GraphqlLimits;
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

/// What [`Skilj::shutdown`] did (docs/architecture.md §123).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Background loops that finished their tick in progress and stopped.
    pub stopped: Vec<&'static str>,
    /// Background loops still busy when the timeout ran out, aborted.
    pub aborted: Vec<&'static str>,
    /// Whether the connection pool finished closing within the timeout -
    /// `false` when requests still held connections at that point.
    pub pool_closed: bool,
}

/// The background loops `SkiljBuilder::build` starts, and the signal that
/// stops them (docs/architecture.md §123). The loops watch the signal only
/// between ticks: a tick in progress - a route submitting a command, a
/// deadline firing - always completes.
#[derive(Clone)]
struct Background {
    stop: Arc<tokio::sync::watch::Sender<bool>>,
    tasks: Arc<std::sync::Mutex<Vec<NamedTask>>>,
}

/// A background loop's name and handle.
type NamedTask = (&'static str, tokio::task::JoinHandle<()>);

impl Background {
    fn new() -> Self {
        Self {
            stop: Arc::new(tokio::sync::watch::channel(false).0),
            tasks: Arc::default(),
        }
    }

    /// Spawns a background loop, handing it the stop signal.
    fn spawn<F, Fut>(&self, name: &'static str, task: F)
    where
        F: FnOnce(StopSignal) -> Fut,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(task(StopSignal(self.stop.subscribe())));
        self.tasks
            .lock()
            .expect("the background task list is never poisoned")
            .push((name, handle));
    }
}

/// A background loop's view of [`Skilj::shutdown`]'s stop request.
struct StopSignal(tokio::sync::watch::Receiver<bool>);

impl StopSignal {
    /// Resolves once a stop is requested. Never, if the `Skilj` that owns
    /// the signal was dropped rather than shut down: dropping it has never
    /// stopped anything, and the routers it handed out may still be serving.
    async fn stopped(&mut self) {
        if self.0.wait_for(|stop| *stop).await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// Sleeps for `duration`, cut short by a stop request - `true` then.
    async fn sleep(&mut self, duration: std::time::Duration) -> bool {
        tokio::select! {
            () = self.stopped() => true,
            () = tokio::time::sleep(duration) => false,
        }
    }
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
    /// `.build()` holds its own clone until `Skilj::shutdown`.
    event_types: Arc<HashMap<(String, String), RegisteredEventType>>,
    /// `Arc`-wrapped for the identical reason `command_types`/
    /// `projections`/`event_types` are - `snapshot_dispatcher()` hands
    /// out a cheap `Arc<dyn SnapshotDispatcher>`, and the background
    /// snapshot catch-up task spawned in `.build()` holds its own clone
    /// until `Skilj::shutdown` ([docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)).
    snapshots: Arc<HashMap<(String, String), RegisteredSnapshot>>,
    /// `bootstrap::generate_bootstrap_secret`'s output, computed once at
    /// `.build()` time and printed then too (see `SkiljBuilder::build`) -
    /// `None` once an active superadmin already exists
    /// (`ClosesPermanentlyOnFirstClaim`). Read only by
    /// `skilj-graphql`'s `createSuperadmin` mutation resolver.
    bootstrap: skilj_core::bootstrap::BootstrapGate,
    /// Shared by every `GraphqlState` built from this `Skilj` - see
    /// `skilj_graphql::GraphqlState::parked_delivery_retry_permits`.
    parked_delivery_retry_permits: Arc<tokio::sync::Semaphore>,
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
    /// `ConsumeEvents`' own `manual_ack` claim lease - see
    /// `SkiljBuilder::read_cursor_checkout_lease`'s own doc comment.
    /// Consulted per-request by `rest_router()`'s `GET /v1/events/consume`
    /// route, the same "stored on `Skilj` itself, not just consumed once
    /// in `.build()`" treatment `projection_query_wait_timeout` already
    /// gets, for the identical reason.
    read_cursor_checkout_lease: std::time::Duration,
    /// `config.max_events_per_read` - see
    /// `SkiljBuilder::max_events_per_read`'s own doc comment. Handed to
    /// both `rest_router()` and the GraphQL state.
    max_events_per_read: usize,
    /// See `SkiljBuilder::graphql_limits`.
    graphql_limits: skilj_graphql::limits::GraphqlLimits,
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
    /// Codeberg issue #32 (round two) - see `skilj_core::command_batcher`'s
    /// own module doc comment for the group-commit design this exists to
    /// run. One shared, process-wide batcher, constructed once in
    /// `.build()`, the same "one instance, not two" treatment
    /// `event_broadcaster`/`event_cache` already get, for the identical
    /// reason: every concurrently-submitted command needs to reach the
    /// one batcher every other concurrent submission does, regardless of
    /// which surface (REST trigger, GraphQL `submitCommand`, parked-
    /// delivery redrive) produced it.
    command_batcher: skilj_core::command_batcher::CommandBatcher,
    /// The background loops `.build()` started, for [`Skilj::shutdown`].
    background: Background,
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

    fn partition_count(&self, bounded_context: &str, projection_name: &str) -> Option<u32> {
        Some(
            self.registered(bounded_context, projection_name)?
                .partition_count,
        )
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

    fn partition_count(&self, bounded_context: &str, snapshot_name: &str) -> Option<u32> {
        let bounded_context = self
            .template_cache
            .effective_bounded_context(bounded_context);
        let registered = self
            .snapshots
            .get(&(bounded_context, snapshot_name.to_string()))?;
        Some(registered.partition_count)
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

/// Codeberg issue #20 - `ScheduleDeadlineDispatcher`'s own implementer,
/// `CrossContextRouteDispatcherImpl`'s identical shape.
struct ScheduleDeadlineDispatcherImpl {
    schedules: Arc<HashMap<String, RegisteredScheduleDeadline>>,
}

impl skilj_core::plugin::ScheduleDeadlineDispatcher for ScheduleDeadlineDispatcherImpl {
    fn schedules(&self) -> Vec<skilj_core::plugin::ScheduleDeadlineInfo> {
        self.schedules.values().map(|s| s.info).collect()
    }

    fn schedule(
        &self,
        schedule_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<skilj_core::plugin::ErasedDeadlineSpec>, serde_json::Error>> {
        let registered = self.schedules.get(schedule_name)?;
        Some((registered.schedule)(source_payload_json))
    }
}

/// Codeberg issue #20 - `CancelDeadlineDispatcher`'s own implementer.
struct CancelDeadlineDispatcherImpl {
    cancels: Arc<HashMap<String, RegisteredCancelDeadline>>,
}

impl skilj_core::plugin::CancelDeadlineDispatcher for CancelDeadlineDispatcherImpl {
    fn cancels(&self) -> Vec<skilj_core::plugin::CancelDeadlineInfo> {
        self.cancels.values().map(|c| c.info).collect()
    }

    fn cancel_tags(
        &self,
        cancel_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<Vec<skilj_core::shared::Tag>>, serde_json::Error>> {
        let registered = self.cancels.get(cancel_name)?;
        Some((registered.cancel_tags)(source_payload_json))
    }
}

impl Skilj {
    /// Stops this `Skilj` (docs/architecture.md §123): its background
    /// loops - projection and snapshot catch-up, routes, deadlines,
    /// scheduled events, key and deadline retention, the cross-instance
    /// listener - each finish the tick they are in and stop, then the
    /// connection pool closes. Whatever is still running when `timeout`
    /// runs out is aborted, and the pool is left to close on its own; the
    /// report says which. An aborted tick is recovered like a crash would
    /// be - routes, deadlines and bridges resume under their idempotency
    /// keys on the next start.
    ///
    /// Stop serving the routers from [`Skilj::rest_router`] and
    /// [`Skilj::graphql_router`] first: they share the pool, so requests
    /// after this fail, and the pool can't finish closing while requests
    /// still hold connections. Dropping a `Skilj` without calling this
    /// stops nothing, as before - the routers may still be in use.
    pub async fn shutdown(self, timeout: std::time::Duration) -> ShutdownReport {
        let deadline = tokio::time::Instant::now() + timeout;
        self.background.stop.send_replace(true);
        let tasks = std::mem::take(
            &mut *self
                .background
                .tasks
                .lock()
                .expect("the background task list is never poisoned"),
        );
        let mut report = ShutdownReport::default();
        for (name, mut handle) in tasks {
            if tokio::time::timeout_at(deadline, &mut handle).await.is_ok() {
                report.stopped.push(name);
            } else {
                handle.abort();
                let _ = handle.await;
                report.aborted.push(name);
            }
        }
        report.pool_closed = tokio::time::timeout_at(deadline, self.pool.close())
            .await
            .is_ok();
        report
    }

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
            schedule_deadlines: HashMap::new(),
            cancel_deadlines: HashMap::new(),
            async_projection_poll_interval: std::time::Duration::from_millis(500),
            snapshot_poll_interval: std::time::Duration::from_millis(500),
            cross_context_route_poll_interval: std::time::Duration::from_millis(500),
            cross_context_route_retry_policy: skilj_retry::RetryPolicy::default(),
            deadline_retry_policy: skilj_retry::RetryPolicy::default(),
            deadline_poll_interval: std::time::Duration::from_millis(500),
            scheduler_poll_interval: std::time::Duration::from_secs(1),
            projection_query_wait_timeout: std::time::Duration::from_secs(5),
            // Codeberg issue #25's investigation (docs/architecture.md
            // §53) - matches `config.read_cursor_checkout_lease`'s own
            // default in specs/skilj.allium.
            read_cursor_checkout_lease: std::time::Duration::from_secs(5 * 60),
            max_events_per_read: skilj_core::event_store::DEFAULT_MAX_EVENTS_PER_READ,
            graphql_limits: skilj_graphql::limits::GraphqlLimits::default(),
            // Codeberg issue #36's own recommendation #3 - matches
            // `command_batcher::DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT`.
            command_batch_idle_in_transaction_timeout: std::time::Duration::from_secs(30),
            command_batch_max_size: skilj_core::command_batcher::DEFAULT_MAX_BATCH_SIZE,
            command_batch_max_concurrent_leaders: None,
            encryption_master_key: None,
            event_broadcast_capacity: 1024,
            event_cache_warm_up_count: 1000,
            pool_options: None,
            // docs/architecture.md §87 - matches
            // `config.idempotency_key_retention` in specs/skilj.allium.
            idempotency_key_retention: Some(DEFAULT_IDEMPOTENCY_KEY_RETENTION),
            deadline_retention: Some(DEFAULT_DEADLINE_RETENTION),
            application_version: None,
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
    ///
    /// Also `None` once a `createSuperadmin` claim has used it - the secret
    /// ends at the first claim, not only while a superadmin is active
    /// (docs/architecture.md §109) - and while a claim is in progress.
    pub fn bootstrap_secret(&self) -> Option<String> {
        self.bootstrap.current()
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
            // Codeberg issue #25 (docs/architecture.md §53) -
            // `chrono::Duration::from_std` only fails for a duration too
            // large to fit, never a real concern for a checkout lease
            // measured in minutes; the same fallback
            // `skilj-kafka`/`skilj-amqp`/`skilj-nats`'s own retry-backoff
            // conversions already use.
            chrono::Duration::from_std(self.read_cursor_checkout_lease)
                .unwrap_or(chrono::Duration::zero()),
            self.max_events_per_read,
            self.command_batcher.clone(),
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
            parked_delivery_retry_permits: self.parked_delivery_retry_permits.clone(),
            bootstrap: self.bootstrap.clone(),
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
            max_events_per_read: self.max_events_per_read,
            limits: self.graphql_limits,
            encryption_master_key: self.encryption_master_key.clone(),
            event_broadcaster: self.event_broadcaster.clone(),
            revocation_broadcaster: self.revocation_broadcaster.clone(),
            event_cache: self.event_cache.clone(),
            template_cache: self.template_cache.clone(),
            command_batcher: self.command_batcher.clone(),
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
    /// `EventType`s/`CommandType`s/`Projection`s this process declares in
    /// an *older* shape than the one already stored - a newer version of the
    /// application registered it (a rolling deploy in progress, or this
    /// process rolled back to). The stored, newer registration is kept
    /// untouched instead of failing startup (docs/architecture.md §101).
    pub kept_newer: Vec<String>,
    /// `EventType`s/`CommandType`s whose stored registration declares a
    /// protection - a sensitive field, a private field, an owner tag key -
    /// that this process's declaration lacks. Startup never removes one
    /// (docs/architecture.md §102): it is kept alongside whatever this
    /// process registers. Removing a protection deliberately takes the
    /// explicit GraphQL registration mutation.
    pub kept_protections: Vec<String>,
}

/// The protections a type's registration declares, with any the stored
/// registration has and `ours` lacks added back - docs/architecture.md
/// §102: startup never weakens a type. A sensitive or private field
/// counts as present when `ours` declares any entry for the same field
/// path (a changed declaration is a change, not a removal); the owner tag
/// key when `ours` names one at all. Returns whether anything was added.
fn keep_stored_protections(
    ours_sensitive: &[skilj_core::shared::SensitiveField],
    ours_private: &[skilj_core::shared::PrivateField],
    ours_owner_tag_key: &Option<String>,
    stored_sensitive: &[skilj_core::shared::SensitiveField],
    stored_private: &[skilj_core::shared::PrivateField],
    stored_owner_tag_key: &Option<String>,
) -> (
    Vec<skilj_core::shared::SensitiveField>,
    Vec<skilj_core::shared::PrivateField>,
    Option<String>,
    bool,
) {
    let mut sensitive = ours_sensitive.to_vec();
    for kept in stored_sensitive {
        if !sensitive.iter().any(|s| s.field == kept.field) {
            sensitive.push(kept.clone());
        }
    }
    let mut private = ours_private.to_vec();
    for kept in stored_private {
        if !private.iter().any(|p| p.field == kept.field) {
            private.push(kept.clone());
        }
    }
    let owner_tag_key = ours_owner_tag_key
        .clone()
        .or_else(|| stored_owner_tag_key.clone());
    let added = sensitive.len() != ours_sensitive.len()
        || private.len() != ours_private.len()
        || owner_tag_key != *ours_owner_tag_key;
    (sensitive, private, owner_tag_key, added)
}

/// The startup registration side of `SkiljBuilder::application_version`
/// (docs/architecture.md §104).
struct RegistrationVersion {
    version: Option<i64>,
    /// Bounded contexts whose `registered_by_version` columns this startup
    /// has already ensured.
    ensured: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl RegistrationVersion {
    fn new(version: Option<u64>) -> Self {
        Self {
            version: version.map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
            ensured: Default::default(),
        }
    }

    /// Whether this process is older than whatever last registered
    /// `name` - only when both have a version.
    async fn is_older_than_stored(
        &self,
        pool: &Pool,
        bounded_context: &str,
        table: skilj_core::db::RegistrationTable,
        name: &str,
    ) -> skilj_core::error::Result<bool> {
        let Some(ours) = self.version else {
            return Ok(false);
        };
        self.ensure_columns(pool, bounded_context).await?;
        Ok(
            skilj_core::db::registration_version(pool, bounded_context, table, name)
                .await?
                .is_some_and(|stored| ours < stored),
        )
    }

    /// Once per bounded context per startup - see
    /// `db::ensure_registration_version_columns`.
    async fn ensure_columns(
        &self,
        pool: &Pool,
        bounded_context: &str,
    ) -> skilj_core::error::Result<()> {
        let first = self
            .ensured
            .lock()
            .expect("never poisoned: nothing panics while holding it")
            .insert(bounded_context.to_string());
        if first {
            skilj_core::db::ensure_registration_version_columns(pool, bounded_context).await?;
        }
        Ok(())
    }

    /// Stamps `name` as registered by this version, if it has one.
    async fn stamp(
        &self,
        pool: &Pool,
        bounded_context: &str,
        table: skilj_core::db::RegistrationTable,
        name: &str,
    ) -> skilj_core::error::Result<()> {
        match self.version {
            Some(version) => {
                self.ensure_columns(pool, bounded_context).await?;
                skilj_core::db::set_registration_version(
                    pool,
                    bounded_context,
                    table,
                    name,
                    version,
                )
                .await
            }
            None => Ok(()),
        }
    }
}

/// docs/architecture.md §101: the two rejections an *older* registration
/// meets when a newer version of the same type is already stored - a
/// field it doesn't declare yet, a tag mapping it doesn't have yet.
fn is_older_than_stored_rejection(error: &skilj_core::Error) -> bool {
    use skilj_core::error::SkiljRejection;
    matches!(
        error.code(),
        "schema_incompatible" | "tag_mapping_key_dropped"
    )
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
    partition_count: u32,
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
        partition_count: T::PARTITION_COUNT,
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
    partition_count: u32,
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
        partition_count: T::PARTITION_COUNT,
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

/// Codeberg issue #20 - one registered `ScheduleDeadline`, the identical
/// type-erased-closure shape `RegisteredCrossContextRoute` already uses
/// for the same reason.
struct RegisteredScheduleDeadline {
    info: skilj_core::plugin::ScheduleDeadlineInfo,
    schedule: fn(&str) -> Result<Option<skilj_core::plugin::ErasedDeadlineSpec>, serde_json::Error>,
}

fn registered_schedule_deadline<S: ScheduleDeadline + 'static>() -> RegisteredScheduleDeadline {
    RegisteredScheduleDeadline {
        info: skilj_core::plugin::ScheduleDeadlineInfo {
            name: S::NAME,
            source_bounded_context: S::Source::BOUNDED_CONTEXT,
            source_event_type: S::Source::NAME,
            target_bounded_context: S::Target::BOUNDED_CONTEXT,
            target_command_type: S::Target::NAME,
            start_from: S::START_FROM,
        },
        schedule: |payload_json| {
            let source_payload: <S::Source as EventType>::Payload =
                serde_json::from_str(payload_json)?;
            match S::schedule(&source_payload) {
                None => Ok(None),
                Some(spec) => Ok(Some(skilj_core::plugin::ErasedDeadlineSpec {
                    fire_at: spec.fire_at,
                    tags: spec.tags,
                    payload_json: serde_json::to_string(&spec.payload)?,
                })),
            }
        },
    }
}

/// Codeberg issue #20 - one registered `CancelDeadline`, `RegisteredScheduleDeadline`'s
/// own counterpart.
struct RegisteredCancelDeadline {
    info: skilj_core::plugin::CancelDeadlineInfo,
    cancel_tags: fn(&str) -> Result<Option<Vec<skilj_core::shared::Tag>>, serde_json::Error>,
}

fn registered_cancel_deadline<C: CancelDeadline + 'static>() -> RegisteredCancelDeadline {
    RegisteredCancelDeadline {
        info: skilj_core::plugin::CancelDeadlineInfo {
            name: C::NAME,
            source_bounded_context: C::Source::BOUNDED_CONTEXT,
            source_event_type: C::Source::NAME,
            deadline_schedule_name: C::Deadline::NAME,
            deadline_schedule_bounded_context:
                <<C::Deadline as ScheduleDeadline>::Source as EventType>::BOUNDED_CONTEXT,
            deadline_schedule_source_event_type:
                <<C::Deadline as ScheduleDeadline>::Source as EventType>::NAME,
            start_from: C::START_FROM,
        },
        cancel_tags: |payload_json| {
            let source_payload: <C::Source as EventType>::Payload =
                serde_json::from_str(payload_json)?;
            Ok(C::cancel_tags(&source_payload))
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
    /// Codeberg issue #20 - keyed by `ScheduleDeadline::NAME`/
    /// `CancelDeadline::NAME` respectively, the identical "spans two
    /// bounded contexts, no single one to key against" reasoning
    /// `cross_context_routes` above already has.
    schedule_deadlines: HashMap<String, RegisteredScheduleDeadline>,
    cancel_deadlines: HashMap<String, RegisteredCancelDeadline>,
    async_projection_poll_interval: std::time::Duration,
    snapshot_poll_interval: std::time::Duration,
    cross_context_route_poll_interval: std::time::Duration,
    /// Codeberg issue #21 - the backoff/attempt-cap policy
    /// `db::catch_up_cross_context_route` applies to a route's own
    /// blocked head-of-line occurrence before parking it. See
    /// `cross_context_route_retry_policy`'s own builder doc comment.
    cross_context_route_retry_policy: skilj_retry::RetryPolicy,
    /// See `deadline_retry_policy`'s builder doc comment.
    deadline_retry_policy: skilj_retry::RetryPolicy,
    deadline_poll_interval: std::time::Duration,
    scheduler_poll_interval: std::time::Duration,
    projection_query_wait_timeout: std::time::Duration,
    read_cursor_checkout_lease: std::time::Duration,
    max_events_per_read: usize,
    graphql_limits: skilj_graphql::limits::GraphqlLimits,
    command_batch_idle_in_transaction_timeout: std::time::Duration,
    command_batch_max_size: usize,
    command_batch_max_concurrent_leaders: Option<usize>,
    encryption_master_key: Option<EncryptionMasterKey>,
    event_broadcast_capacity: usize,
    event_cache_warm_up_count: usize,
    pool_options: Option<skilj_core::db::PgPoolOptions>,
    idempotency_key_retention: Option<std::time::Duration>,
    deadline_retention: Option<std::time::Duration>,
    application_version: Option<u64>,
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

    /// Codeberg issue #20 - registers a [`ScheduleDeadline`], the same
    /// "never scoped by `.bounded_context(...)`, keyed by its own
    /// `NAME`, last registration wins" treatment `cross_context_route`
    /// just above already gives its own trait.
    pub fn schedule_deadline<S: ScheduleDeadline + 'static>(mut self) -> Self {
        self.schedule_deadlines
            .insert(S::NAME.to_string(), registered_schedule_deadline::<S>());
        self
    }

    /// Codeberg issue #20 - registers a [`CancelDeadline`],
    /// `schedule_deadline`'s own counterpart.
    pub fn cancel_deadline<C: CancelDeadline + 'static>(mut self) -> Self {
        self.cancel_deadlines
            .insert(C::NAME.to_string(), registered_cancel_deadline::<C>());
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

    /// Codeberg issue #21 - the backoff/attempt-cap policy
    /// `db::catch_up_cross_context_route` applies to a route's own
    /// blocked head-of-line occurrence: how long to wait before
    /// re-attempting a failed target-command submission, and how many
    /// attempts (or how much elapsed time) to allow before giving up and
    /// recording it as a `ParkedDelivery` instead of blocking the route
    /// forever. Defaults to `skilj_retry::RetryPolicy::default()` - 1s
    /// initial backoff, doubling, capped at 5 minutes, 5 attempts. See
    /// docs/architecture.md's parked-deliveries section.
    pub fn cross_context_route_retry_policy(mut self, policy: skilj_retry::RetryPolicy) -> Self {
        self.cross_context_route_retry_policy = policy;
        self
    }

    /// How a deadline whose command fails with an error (not a business
    /// rejection) is retried before it's recorded as a `ParkedDelivery`
    /// (kind `DEADLINE`) in the target bounded context, where
    /// `retryParkedDelivery`/`discardParkedDelivery` handle it.
    /// Defaults to `skilj_retry::RetryPolicy::default()` - 1s initial
    /// backoff, doubling, capped at 5 minutes, 5 attempts; a retry also
    /// waits for the next fire tick (`deadline_poll_interval`).
    /// docs/architecture.md §115.
    pub fn deadline_retry_policy(mut self, policy: skilj_retry::RetryPolicy) -> Self {
        self.deadline_retry_policy = policy;
        self
    }

    /// Codeberg issue #20 - how often each of the three background tasks
    /// backing [`ScheduleDeadline`]/[`CancelDeadline`] runs its own tick
    /// (catching up every registered schedule, catching up every
    /// registered cancel reactor, and scanning every bounded context's
    /// own `deadlines` table for due rows to fire) - one shared knob
    /// rather than three, the same "one interval per feature, not one per
    /// internal task" register every other `*_poll_interval` here already
    /// keeps. Defaults to 500ms, matching every sibling default.
    pub fn deadline_poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.deadline_poll_interval = interval;
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

    /// How long a `GET /v1/events/consume?mode=manual`'s claim on a
    /// `manual_ack` `ReadCursor` holds before a second concurrent caller
    /// for the same token may reclaim it - `config.read_cursor_checkout_lease`
    /// in specs/skilj.allium, `ReadCursor.checked_out_at`'s own spec doc
    /// comment. Codeberg issue #25's investigation (docs/architecture.md
    /// §53): closes the gap where two bridge instances of the same
    /// `skilj-kafka`/`skilj-amqp`/`skilj-nats` `OutboundMapping` (or any
    /// other manual-ack REST consumer) could both be served, and both act
    /// on, the same unacknowledged batch. Defaults to 5 minutes, matching
    /// the spec's own default - generous enough that an ordinary caller's
    /// poll-serve-act-acknowledge cycle never trips it, short enough that
    /// a caller which crashes after being served does not leave the
    /// stream stuck for long. Meaningless for `auto_advance`, which has no
    /// equivalent gap (serving already advances its own cursor in the
    /// same moment) - this only ever affects `manual_ack` cursors.
    pub fn read_cursor_checkout_lease(mut self, lease: std::time::Duration) -> Self {
        self.read_cursor_checkout_lease = lease;
        self
    }

    /// The most events one `GET /v1/events`, `GET /v1/events/consume` or
    /// GraphQL `queryEvents` returns - `config.max_events_per_read` in
    /// specs/skilj.allium. Each serves the first that many matching
    /// events in sequence order, and a caller continues from the last
    /// one: `after`/`nextCursor` for `GET /v1/events`, `afterSequence`
    /// for `queryEvents`, and the server-side cursor for consume (which
    /// the bridges already poll in a loop). Keeps one read of a long
    /// history - a new consumer starting from the beginning, a fetch
    /// with no `after` - from loading and returning all of it at once.
    /// Defaults to 1000 (`DEFAULT_MAX_EVENTS_PER_READ`). Clamped to at
    /// least 1: a page of zero could never make progress.
    pub fn max_events_per_read(mut self, max: usize) -> Self {
        self.max_events_per_read = max.max(1);
        self
    }

    /// Per-request bounds on the GraphQL endpoint - largest accepted
    /// body (413 beyond it, checked before authentication), deepest
    /// query, most fields per query, and how many history-scanning fields
    /// (`queryEvents`, `countEvents`, `fetchCommands`, `projection`) one
    /// request may select, aliases included. See
    /// [`skilj_graphql::limits::GraphqlLimits`] for the defaults, which fit
    /// the standard introspection query GraphiQL and codegen tools send.
    pub fn graphql_limits(mut self, limits: skilj_graphql::limits::GraphqlLimits) -> Self {
        self.graphql_limits = limits;
        self
    }

    /// A batch leader's own `SET LOCAL idle_in_transaction_session_timeout`,
    /// on the transaction holding the bounded-context lock for the whole
    /// batch it's processing - `skilj_core::command_batcher::CommandBatcher
    /// ::with_idle_in_transaction_timeout`'s own doc comment for the full
    /// design (Codeberg issue #36's own recommendation #3: a defense-in-
    /// depth backstop, not a feature to tune for ordinary throughput).
    /// Defaults to 30 seconds - generous for any real batch, short enough
    /// that a genuinely stuck leader's lock is released well before an
    /// operator would otherwise notice a bounded context has wedged.
    pub fn command_batch_idle_in_transaction_timeout(
        mut self,
        timeout: std::time::Duration,
    ) -> Self {
        self.command_batch_idle_in_transaction_timeout = timeout;
        self
    }

    /// The most commands one bounded-context lock acquisition processes
    /// (default 256) - see `CommandBatcher::with_max_batch_size` and
    /// docs/performance.md.
    pub fn command_batch_max_size(mut self, max_size: usize) -> Self {
        self.command_batch_max_size = max_size;
        self
    }

    /// The most batch leaders running at once across all bounded contexts
    /// (default: half the pool's `max_connections`) - see
    /// `CommandBatcher::with_max_concurrent_leaders` and
    /// docs/performance.md.
    pub fn command_batch_max_concurrent_leaders(mut self, max_leaders: usize) -> Self {
        self.command_batch_max_concurrent_leaders = Some(max_leaders);
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
    ///
    /// Clamped to at least 1 - a zero-capacity broadcast channel can't exist.
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
    ///
    /// `0` turns the cache off: every read goes to Postgres.
    pub fn event_cache_warm_up_count(mut self, count: usize) -> Self {
        self.event_cache_warm_up_count = count;
        self
    }

    /// How long a recorded idempotency key keeps deduplicating
    /// (docs/architecture.md §87): a submission that repeats a key within
    /// this window after it was first accepted gets the original outcome
    /// back; after it, the key is deleted and a submission bearing it is
    /// processed as a new command. Defaults to
    /// [`DEFAULT_IDEMPOTENCY_KEY_RETENTION`] (one hour). A key is kept for
    /// *at least* this long - a background task deletes expired keys
    /// about once a minute (or once per `retention`, if shorter), starting
    /// `min(retention, 1 hour)` after the instance starts, so recovery
    /// after an outage longer than the retention still finds its keys.
    ///
    /// Set it longer than the longest time anything may retry the same
    /// key, because a retry after the window lands twice. That includes:
    /// a client or workflow engine retrying a request it never heard back
    /// from (a Temporal activity's retry policy, say); a Kafka/AMQP/NATS
    /// bridge re-reading old broker messages (a consumer group reset to an
    /// earlier offset); a `CrossContextRoute` whose retry policy
    /// (`cross_context_route_retry_policy`) keeps retrying one delivery
    /// past the window; and `retryParkedDelivery`, which redrives under
    /// the original attempt's key so that an attempt which committed but
    /// reported failure isn't applied twice - an operator redriving after
    /// the window loses that protection.
    pub fn idempotency_key_retention(mut self, retention: std::time::Duration) -> Self {
        self.idempotency_key_retention = Some(retention);
        self
    }

    /// This application's version, for startup registration
    /// (docs/architecture.md §104): a number that only ever increases from
    /// one release to the next - a build number, or a release's own
    /// ordinal. Each `EventType`/`CommandType`/`Projection` row this
    /// process registers at startup is stamped with it, and a process
    /// whose version is *lower* than a row's stamp leaves that row exactly
    /// as it is, reporting it in `ReconciliationReport::kept_newer`. That
    /// is what lets two versions run side by side - a rolling deploy, a
    /// rollback, an old instance restarting mid-rollout - without each
    /// startup undoing the other's registrations: flags, projection
    /// consumed event types (each flip a full projection rebuild), and
    /// everything else the compatibility rules don't cover.
    ///
    /// Unset (the default), startup registers exactly as without it, and
    /// leaves any existing stamp in place. Whatever the version, startup
    /// never removes a protection (§102).
    pub fn application_version(mut self, version: u64) -> Self {
        self.application_version = Some(version);
        self
    }

    /// How long a resolved deadline (fired, cancelled, parked or
    /// forgotten) is kept before a background task deletes it
    /// (docs/architecture.md §163). Defaults to
    /// [`DEFAULT_DEADLINE_RETENTION`] (30 days). Pending and firing
    /// deadlines are never deleted. A resolved row holds no payload (§162),
    /// only when and how it resolved; what it did is in the events and
    /// commands it submitted.
    pub fn deadline_retention(mut self, retention: std::time::Duration) -> Self {
        self.deadline_retention = Some(retention);
        self
    }

    /// Never delete resolved deadlines: each bounded context's `deadlines`
    /// table keeps one row per deadline ever scheduled.
    pub fn keep_resolved_deadlines_forever(mut self) -> Self {
        self.deadline_retention = None;
        self
    }

    /// Never delete recorded idempotency keys (the behaviour before
    /// retention existed): a key deduplicates forever, and the
    /// `idempotency_keys` table grows with every keyed submission.
    pub fn keep_idempotency_keys_forever(mut self) -> Self {
        self.idempotency_key_retention = None;
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

    /// Convenience method for production pool settings:
    /// - `max_connections`: 2x CPU cores (rule of thumb from eventcore-postgres)
    /// - `min_connections`: half of max (warm pool, avoids connection churn)
    /// - `acquire_timeout`: 30s (fail fast under contention)
    /// - `idle_timeout`: 10min (reclaim idle connections)
    ///
    /// Override any setting by chaining `.pool_options()` after this:
    /// `.pool_options_performance_optimized().pool_options(custom_options)`
    pub fn pool_options_performance_optimized(self) -> Self {
        use std::thread::available_parallelism;
        let cpu_count = available_parallelism().map(|n| n.get()).unwrap_or(4);
        let max_connections = (cpu_count * 2) as u32;
        let min_connections = (max_connections / 2).max(1);
        self.pool_options(
            skilj_core::db::PgPoolOptions::new()
                .max_connections(max_connections)
                .min_connections(min_connections)
                .acquire_timeout(std::time::Duration::from_secs(30))
                .idle_timeout(std::time::Duration::from_secs(10 * 60)),
        )
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
        // Shared by `Skilj` and every `GraphqlState` built from it, so a
        // claim through any of them consumes it (docs/architecture.md §109).
        let bootstrap = skilj_core::bootstrap::BootstrapGate::new(bootstrap_secret.clone());
        // Half the pool, at least one - the share `CommandBatcher` gives
        // its leaders and the route task its ticks (docs/architecture.md
        // §117).
        let parked_delivery_retry_permits = Arc::new(tokio::sync::Semaphore::new(
            (pool.options().get_max_connections() as usize / 2).max(1),
        ));
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

        // Codeberg issue #13: reconciliation (after the schema patches
        // below) never looks up a templated tenant's own name (it only
        // ever loops over the literal keys this process's own
        // `.bounded_context(name)`/`#[auto_register]` calls declared,
        // always a template's name or an ordinary untemplated one -
        // never a tenant's, chosen later at runtime), so its
        // `reconciliation_dispatcher` gets a real clone of this same cache for structural
        // consistency, even though every lookup it makes resolves to
        // itself.
        let bounded_contexts_for_warm_up = skilj_core::db::list_bounded_contexts(&pool).await?;
        let template_cache = skilj_core::template_cache::TemplateCache::new();
        template_cache.refresh(&pool).await?;

        let identity_provider = self.identity_provider.map(|config| {
            Arc::new(IdentityProvider {
                cache: Arc::new(JwksCache::new(config.jwks_endpoint.clone())),
                config,
            })
        });

        let poll_interval = self.async_projection_poll_interval;
        let projection_query_wait_timeout = self.projection_query_wait_timeout;
        let read_cursor_checkout_lease = self.read_cursor_checkout_lease;
        let max_events_per_read = self.max_events_per_read;
        let graphql_limits = self.graphql_limits;
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
        let command_batcher = skilj_core::command_batcher::CommandBatcher::new()
            .with_idle_in_transaction_timeout(self.command_batch_idle_in_transaction_timeout)
            .with_max_batch_size(self.command_batch_max_size);
        let command_batcher = match self.command_batch_max_concurrent_leaders {
            Some(n) => command_batcher.with_max_concurrent_leaders(n),
            None => command_batcher,
        };
        stream::iter(bounded_contexts_for_warm_up)
            .map(|bc| {
                let pool = &pool;
                let event_cache = &event_cache;
                async move {
                    let prepared = async {
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
                        skilj_core::db::ensure_projection_state_owner_columns(pool, &bc.name)
                            .await?;
                        // Codeberg issue #25: `projection_state.as_of_sequence`/
                        // `projection_rebuild_state.as_of_sequence`, the real
                        // fix for a genuine cross-instance double-fold race
                        // found while investigating that issue - see
                        // `ensure_projection_state_as_of_sequence_columns`'s
                        // own doc comment.
                        skilj_core::db::ensure_projection_state_as_of_sequence_columns(
                            pool, &bc.name,
                        )
                        .await?;
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
                        // Codeberg issue #20: native one-shot, per-entity
                        // deadlines - same "patched into every bounded
                        // context, every startup" treatment, for the new
                        // `deadline_cursors`/`deadlines` tables. See
                        // `ensure_deadline_cursors_table`/`ensure_deadlines_table`'s
                        // own doc comments.
                        skilj_core::db::ensure_deadline_cursors_table(pool, &bc.name).await?;
                        skilj_core::db::ensure_deadlines_table(pool, &bc.name).await?;
                        // Codeberg issue #25: partitioned async Projection
                        // catch-up - same "patched into every bounded
                        // context, every startup" treatment, for a brand-new
                        // table that needs no migration dance. See
                        // `ensure_projection_partition_progress_table`'s own
                        // doc comment.
                        skilj_core::db::ensure_projection_partition_progress_table(pool, &bc.name)
                            .await?;
                        // Codeberg issue #25 (docs/architecture.md §52):
                        // partitioned Snapshot catch-up - same treatment,
                        // for `Snapshot`'s own twin table. See
                        // `ensure_snapshot_partition_progress_table`'s own
                        // doc comment.
                        skilj_core::db::ensure_snapshot_partition_progress_table(pool, &bc.name)
                            .await?;
                        // Codeberg issue #25 (docs/architecture.md §53): the
                        // read_cursors checkout mechanism closing the
                        // Kafka/AMQP/NATS outbound bridges' double-publish
                        // gap. See `ensure_read_cursors_checkout_column`'s
                        // own doc comment.
                        skilj_core::db::ensure_read_cursors_checkout_column(pool, &bc.name).await?;
                        // External-message dedup (docs/architecture.md §39,
                        // specs/skilj.allium's own rule CreateExternalEvent) -
                        // same "patched into every bounded context, every
                        // startup" treatment, for a brand-new table that needs
                        // no migration dance. See
                        // `ensure_external_message_cursors_table`'s own doc
                        // comment.
                        skilj_core::db::ensure_external_message_cursors_table(pool, &bc.name)
                            .await?;
                        // Its per-message counterpart (docs/architecture.md §175).
                        skilj_core::db::ensure_external_message_keys_table(pool, &bc.name).await?;
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
                        skilj_core::db::ensure_correlation_causation_columns(pool, &bc.name)
                            .await?;
                        // Codeberg issue #21 (dead-letter/parking for failed
                        // event/message handler delivery) - `cross_context_route_cursors`'
                        // own three new retry-backoff columns, same "patched
                        // into every bounded context, every startup"
                        // treatment. See
                        // `ensure_cross_context_route_retry_columns`'s own
                        // doc comment for why this is a separate `ALTER
                        // TABLE` patch rather than folded into
                        // `ensure_cross_context_route_cursors_table` above.
                        skilj_core::db::ensure_cross_context_route_retry_columns(pool, &bc.name)
                            .await?;
                        // Same pass's own `parked_deliveries` table - a
                        // brand-new table, so (unlike the retry columns just
                        // above) this needs no `ALTER TABLE` patch, only the
                        // identical `CREATE TABLE IF NOT EXISTS` every other
                        // brand-new per-bounded-context table already gets
                        // here.
                        skilj_core::db::ensure_parked_deliveries_table(pool, &bc.name).await?;
                        // Codeberg issue #25 review (docs/architecture.md
                        // §56): the concurrent-instance parked-delivery
                        // duplication fix's own belt-and-suspenders unique
                        // index, plus the one-time dedup a bounded context
                        // that already hit the race needs before that index
                        // can even be created. See
                        // `migrate_parked_deliveries_dedup_and_unique_index`'s
                        // own doc comment.
                        skilj_core::db::migrate_parked_deliveries_dedup_and_unique_index(
                            pool, &bc.name,
                        )
                        .await?;
                        // Last: it reads `events` as the patches above
                        // leave it - a bounded context from before
                        // `metadata_correlation_id`/`metadata_causation_id`
                        // failed here first, and never got them
                        // (docs/architecture.md §158).
                        event_cache.warm(pool, &bc.name).await?;
                        Ok::<(), skilj_core::Error>(())
                    }
                    .await;
                    // docs/architecture.md §157: `bounded_contexts_for_warm_up`
                    // was read before any of this. Another instance may
                    // have hard-deleted the bounded context since, and
                    // this instance's startup is no reason to fail.
                    match prepared {
                        Err(err)
                            if skilj_core::db::get_bounded_context(pool, &bc.name)
                                .await?
                                .is_none() =>
                        {
                            tracing::info!(
                                bounded_context = %bc.name,
                                error = %err,
                                "bounded context deleted during startup, skipped"
                            );
                            Ok(())
                        }
                        other => other,
                    }
                }
            })
            .buffer_unordered(BACKGROUND_TASK_CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await?;

        // After the schema patches above (docs/architecture.md §159):
        // reconciliation reads each declared bounded context's
        // registration tables, which a bounded context from an older
        // version only has every column of once they've run.
        let mut report = ReconciliationReport::default();
        if let Some(external_subject) = &self.reconciliation_role {
            let role = skilj_core::access_control::resolve_role_by_external_subject(
                external_subject,
                &roles,
            )?
            .clone();

            let version = RegistrationVersion::new(self.application_version);
            reconcile_event_types(
                &pool,
                &role,
                &self.event_types,
                &mut report,
                chrono::Utc::now(),
                &version,
            )
            .await?;
            reconcile_command_types(&pool, &role, &command_types, &mut report, &version).await?;
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
                &version,
            )
            .await?;
        }

        // The initial GraphQL schema (Codeberg issue #2; `@guarantee
        // RegistrationReachesEveryInstance`) - built here, ahead of
        // `Skilj` itself, the same "can't call `self.graphql_state()`
        // before `self` exists" reasoning `reconciliation_dispatcher`
        // above already works around, applied to the whole `GraphqlState`
        // this time rather than just one dispatcher.
        let schema_registry = Arc::new(
            skilj_graphql::schema::SchemaRegistry::build(skilj_graphql::GraphqlState {
                pool: pool.clone(),
                parked_delivery_retry_permits: parked_delivery_retry_permits.clone(),
                bootstrap: bootstrap.clone(),
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
                max_events_per_read,
                limits: graphql_limits,
                encryption_master_key: encryption_master_key.clone(),
                event_broadcaster: event_broadcaster.clone(),
                revocation_broadcaster: revocation_broadcaster.clone(),
                event_cache: event_cache.clone(),
                template_cache: template_cache.clone(),
                command_batcher: command_batcher.clone(),
            })
            .await?,
        );

        let background = Background::new();
        let skilj = Skilj {
            pool,
            command_types,
            projections,
            snapshots,
            event_types: Arc::new(self.event_types),
            bootstrap,
            parked_delivery_retry_permits,
            identity_provider,
            projection_query_wait_timeout,
            read_cursor_checkout_lease,
            max_events_per_read,
            graphql_limits,
            encryption_master_key,
            event_broadcaster,
            revocation_broadcaster,
            event_cache,
            schema_registry,
            template_cache,
            command_batcher,
            background: background.clone(),
        };

        // The single shared background task backing §8 item 6's async
        // case - one task, not one per bounded context, since a bounded
        // context can gain its first async Projection/ProjectionRebuild
        // at any point after this returns (via GraphQL `registerProjection`),
        // not only at build time. Runs until `Skilj::shutdown` (§123). Runs its first
        // catch-up immediately, before the first sleep, so a caller
        // creating an event right after `.build()` returns doesn't also
        // pay for a full idle poll interval on top of processing time.
        let poll_pool = skilj.pool.clone();
        let poll_dispatcher = skilj.projection_dispatcher();
        background.spawn("async_projections", move |mut stop| async move {
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
                                    contain_panic("async_projection", bc.name.clone(), async move {
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
                                    })
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
                if stop.sleep(poll_interval).await {
                    return;
                }
            }
        });

        // docs/architecture.md §19's own background task - one shared
        // task, not one per bounded context, for the identical reasons
        // the async projection task above is. Runs until `Skilj::shutdown`,
        // same as every other background task here.
        // Deliberately its own task, not folded into the async
        // projection one above even though the shape rhymes closely -
        // see `skilj_core::plugin::Snapshot`'s own doc comment for why
        // `Snapshot` stays structurally separate from `Projection`
        // throughout.
        // docs/architecture.md §87: idempotency-key retention. One shared
        // task like the others; not spawned at all when keys are kept
        // forever.
        if let Some(retention) = self.idempotency_key_retention {
            let retention_pool = skilj.pool.clone();
            let cleanup_interval = retention
                .min(IDEMPOTENCY_KEY_CLEANUP_INTERVAL)
                .max(std::time::Duration::from_secs(1));
            let startup_grace = retention.min(IDEMPOTENCY_KEY_STARTUP_GRACE);
            let retention = chrono::Duration::from_std(retention).unwrap_or(chrono::Duration::MAX);
            background.spawn("idempotency_key_retention", move |mut stop| async move {
                // docs/architecture.md §94: recovery paths that retry under
                // a key (a reclaimed deadline, a route re-reading its
                // source, a broker redelivering an uncommitted message)
                // all run right after startup. After an outage longer
                // than the retention they must find their keys, not race
                // this task deleting them - so it waits before its first
                // sweep.
                if stop.sleep(startup_grace).await {
                    return;
                }
                loop {
                    let start = std::time::Instant::now();
                    retention_tick(&retention_pool, RetentionTarget::IdempotencyKeys, retention)
                        .instrument(tracing::info_span!("idempotency_key_retention_tick"))
                        .await;
                    BACKGROUND_TASK_TICK_DURATION.record(
                        start.elapsed().as_secs_f64(),
                        &[KeyValue::new("task", "idempotency_key_retention")],
                    );
                    if stop.sleep(cleanup_interval).await {
                        return;
                    }
                }
            });
        }

        // docs/architecture.md §163: resolved-deadline retention. No
        // startup grace: nothing retries under a resolved deadline.
        if let Some(retention) = self.deadline_retention {
            let retention_pool = skilj.pool.clone();
            let cleanup_interval = retention
                .min(DEADLINE_CLEANUP_INTERVAL)
                .max(std::time::Duration::from_secs(1));
            let retention = chrono::Duration::from_std(retention).unwrap_or(chrono::Duration::MAX);
            background.spawn("deadline_retention", move |mut stop| async move {
                loop {
                    let start = std::time::Instant::now();
                    retention_tick(
                        &retention_pool,
                        RetentionTarget::ResolvedDeadlines,
                        retention,
                    )
                    .instrument(tracing::info_span!("deadline_retention_tick"))
                    .await;
                    BACKGROUND_TASK_TICK_DURATION.record(
                        start.elapsed().as_secs_f64(),
                        &[KeyValue::new("task", "deadline_retention")],
                    );
                    if stop.sleep(cleanup_interval).await {
                        return;
                    }
                }
            });
        }

        let snapshot_pool = skilj.pool.clone();
        let snapshot_dispatcher = skilj.snapshot_dispatcher();
        let snapshot_interval = self.snapshot_poll_interval;
        background.spawn("snapshots", move |mut stop| async move {
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
                                    contain_panic("snapshot", bc.name.clone(), async move {
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
                                    })
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
                if stop.sleep(snapshot_interval).await {
                    return;
                }
            }
        });

        // The background task driving `CrossContextRoute`s - this
        // crate's own answer to "make messages cross bounded contexts
        // without needing an external system like Temporal" (see
        // `skilj_core::plugin::CrossContextRoute`'s own doc comment for
        // why this stays a single-hop reaction, not a Saga/process
        // manager). One shared task, not one per route, for the same
        // reasons the async projection task above is; runs until
        // `Skilj::shutdown`, same as every other background task here. The route list itself is fixed at `.build()` time
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
        let route_retry_policy = self.cross_context_route_retry_policy;
        // A route tick holds its advisory lock on a connection of its own
        // for the whole tick, and its work needs further connections. At
        // most half the pool (at least one) is ever held that way, the
        // same share `CommandBatcher` gives its leaders, so ticks can't
        // take every connection and then wait on each other for one more
        // (docs/architecture.md §117).
        let route_concurrency = (route_pool.options().get_max_connections() as usize / 2)
            .clamp(1, BACKGROUND_TASK_CONCURRENCY);
        background.spawn("cross_context_routes", move |mut stop| async move {
            loop {
                let start = std::time::Instant::now();
                async {
                    stream::iter(routes.clone())
                        .for_each_concurrent(route_concurrency, |route| {
                            let route_pool = route_pool.clone();
                            let route_dispatcher = route_dispatcher.clone();
                            let route_command_dispatcher = route_command_dispatcher.clone();
                            let route_projection_dispatcher = route_projection_dispatcher.clone();
                            let route_snapshot_dispatcher = route_snapshot_dispatcher.clone();
                            let route_broadcaster = route_broadcaster.clone();
                            let route_event_cache = route_event_cache.clone();
                            let route_encryption_master_key = route_encryption_master_key.clone();
                            contain_panic(
                                "cross_context_route",
                                route.name.to_string(),
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
                                        &route_retry_policy,
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
                                },
                            )
                        })
                        .await;
                }
                .instrument(tracing::info_span!("cross_context_route_tick"))
                .await;
                BACKGROUND_TASK_TICK_DURATION.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("task", "cross_context_route")],
                );
                if stop.sleep(route_interval).await {
                    return;
                }
            }
        });

        // Codeberg issue #20: native one-shot, per-entity deadlines -
        // three more shared background tasks, the same "one task, not
        // one per registration, detached, runs for the process's
        // lifetime" treatment the `CrossContextRoute` task just above
        // already gets. `schedule_deadlines`/`cancel_deadlines` are each
        // read once here too, for the identical "no runtime registration
        // surface" reason `routes` is above. All three share one
        // `deadline_poll_interval` - see that builder method's own doc
        // comment for why.
        let deadline_interval = self.deadline_poll_interval;

        let schedule_deadline_pool = skilj.pool.clone();
        let schedule_deadline_event_cache = skilj.event_cache.clone();
        let schedule_deadline_dispatcher: Arc<dyn skilj_core::plugin::ScheduleDeadlineDispatcher> =
            Arc::new(ScheduleDeadlineDispatcherImpl {
                schedules: Arc::new(self.schedule_deadlines),
            });
        let schedules = schedule_deadline_dispatcher.schedules();
        let scheduled: Vec<(&'static str, &'static str)> = schedules
            .iter()
            .map(|schedule| (schedule.name, schedule.source_bounded_context))
            .collect();
        background.spawn("schedule_deadlines", move |mut stop| async move {
            loop {
                let start = std::time::Instant::now();
                async {
                    stream::iter(schedules.clone())
                        .for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, |schedule| {
                            let schedule_deadline_pool = schedule_deadline_pool.clone();
                            let schedule_deadline_dispatcher = schedule_deadline_dispatcher.clone();
                            let schedule_deadline_event_cache =
                                schedule_deadline_event_cache.clone();
                            contain_panic(
                                "schedule_deadline",
                                schedule.name.to_string(),
                                async move {
                                    if let Err(e) = skilj_core::db::catch_up_schedule_deadline(
                                        &schedule_deadline_pool,
                                        &schedule,
                                        schedule_deadline_dispatcher.as_ref(),
                                        &schedule_deadline_event_cache,
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            schedule = %schedule.name,
                                            error = %e,
                                            "schedule deadline catch-up failed"
                                        );
                                        BACKGROUND_TASK_ERRORS.add(
                                            1,
                                            &[
                                                KeyValue::new("task", "schedule_deadline"),
                                                KeyValue::new("reason", "catch_up_failed"),
                                            ],
                                        );
                                    }
                                },
                            )
                        })
                        .await;
                }
                .instrument(tracing::info_span!("schedule_deadline_tick"))
                .await;
                BACKGROUND_TASK_TICK_DURATION.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("task", "schedule_deadline")],
                );
                if stop.sleep(deadline_interval).await {
                    return;
                }
            }
        });

        let cancel_deadline_pool = skilj.pool.clone();
        let cancel_deadline_event_cache = skilj.event_cache.clone();
        let cancel_deadline_dispatcher: Arc<dyn skilj_core::plugin::CancelDeadlineDispatcher> =
            Arc::new(CancelDeadlineDispatcherImpl {
                cancels: Arc::new(self.cancel_deadlines),
            });
        let cancels = cancel_deadline_dispatcher.cancels();
        let fire_cancels = cancels.clone();
        // docs/architecture.md §130: a cancel waits for its schedule to
        // have processed what precedes each cancelling event. Nothing in
        // this process runs a schedule that isn't registered here, so its
        // cancels would wait on another process doing it - or forever.
        for cancel in &cancels {
            let scheduled_here = scheduled.contains(&(
                cancel.deadline_schedule_name,
                cancel.deadline_schedule_bounded_context,
            ));
            if !scheduled_here {
                tracing::warn!(
                    cancel = cancel.name,
                    schedule = cancel.deadline_schedule_name,
                    "a cancel deadline's schedule isn't registered in this process - its \
                     cancels wait until something runs that schedule"
                );
            }
        }
        background.spawn("cancel_deadlines", move |mut stop| async move {
            loop {
                let start = std::time::Instant::now();
                async {
                    stream::iter(cancels.clone())
                        .for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, |cancel| {
                            let cancel_deadline_pool = cancel_deadline_pool.clone();
                            let cancel_deadline_dispatcher = cancel_deadline_dispatcher.clone();
                            let cancel_deadline_event_cache = cancel_deadline_event_cache.clone();
                            contain_panic("cancel_deadline", cancel.name.to_string(), async move {
                                if let Err(e) = skilj_core::db::catch_up_cancel_deadline(
                                    &cancel_deadline_pool,
                                    &cancel,
                                    cancel_deadline_dispatcher.as_ref(),
                                    &cancel_deadline_event_cache,
                                )
                                .await
                                {
                                    tracing::warn!(
                                        cancel = %cancel.name,
                                        error = %e,
                                        "cancel deadline catch-up failed"
                                    );
                                    BACKGROUND_TASK_ERRORS.add(
                                        1,
                                        &[
                                            KeyValue::new("task", "cancel_deadline"),
                                            KeyValue::new("reason", "catch_up_failed"),
                                        ],
                                    );
                                }
                            })
                        })
                        .await;
                }
                .instrument(tracing::info_span!("cancel_deadline_tick"))
                .await;
                BACKGROUND_TASK_TICK_DURATION.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("task", "cancel_deadline")],
                );
                if stop.sleep(deadline_interval).await {
                    return;
                }
            }
        });

        // Unlike the two tasks just above, not tied to a fixed,
        // read-once-at-startup list - `deadline_fire_tick` fans out over
        // every *currently active* bounded context each tick instead,
        // the same register `scheduler_tick` below already uses for its
        // own due-occurrence scan (`db::fire_due_deadlines`'s own doc
        // comment explains why).
        let deadline_fire_pool = skilj.pool.clone();
        let deadline_fire_command_dispatcher = skilj.command_dispatcher();
        let deadline_fire_projection_dispatcher = skilj.projection_dispatcher();
        let deadline_fire_snapshot_dispatcher = skilj.snapshot_dispatcher();
        let deadline_fire_broadcaster = skilj.event_broadcaster.clone();
        let deadline_fire_event_cache = skilj.event_cache.clone();
        let deadline_fire_encryption_master_key = skilj.encryption_master_key.clone();
        let deadline_retry_policy = self.deadline_retry_policy;
        background.spawn("fire_deadlines", move |mut stop| async move {
            let fire_cancels = fire_cancels;
            loop {
                let start = std::time::Instant::now();
                deadline_fire_tick(
                    &deadline_fire_pool,
                    deadline_fire_command_dispatcher.as_ref(),
                    deadline_fire_projection_dispatcher.as_ref(),
                    deadline_fire_snapshot_dispatcher.as_ref(),
                    &deadline_fire_broadcaster,
                    &deadline_fire_event_cache,
                    deadline_fire_encryption_master_key.as_ref(),
                    chrono::Utc::now(),
                    &deadline_retry_policy,
                    &fire_cancels,
                )
                .instrument(tracing::info_span!("deadline_fire_tick"))
                .await;
                BACKGROUND_TASK_TICK_DURATION.record(
                    start.elapsed().as_secs_f64(),
                    &[KeyValue::new("task", "deadline_fire")],
                );
                if stop.sleep(deadline_interval).await {
                    return;
                }
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
        background.spawn("system_event_scheduler", move |mut stop| async move {
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
                if stop.sleep(scheduler_interval).await {
                    return;
                }
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
        background.spawn("cross_instance", move |mut stop| async move {
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
                        if stop.sleep(std::time::Duration::from_secs(5)).await {
                            return;
                        }
                    }
                }
            };
            loop {
                // Waiting for a notification is this loop's idle point.
                let received = tokio::select! {
                    () = stop.stopped() => return,
                    received = listener
                        .recv()
                        .instrument(tracing::info_span!("cross_instance_recv")) => received,
                };
                let message = match received {
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
                        if stop.sleep(std::time::Duration::from_secs(1)).await {
                            return;
                        }
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
                    message @ (skilj_core::cross_instance::Message::RegistrationChanged
                    | skilj_core::cross_instance::Message::Resync) => {
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
                        // docs/architecture.md §83: notifications were lost
                        // while the listener was reconnecting. Signalled
                        // after the rebuild above, so a subscriber that
                        // reconnects in response finds the fresh schema.
                        if matches!(message, skilj_core::cross_instance::Message::Resync) {
                            cross_instance_event_broadcaster.signal_gap();
                        }
                    }
                }
            }
        });

        Ok((skilj, report))
    }
}

/// What a retention task deletes.
#[derive(Clone, Copy)]
enum RetentionTarget {
    /// docs/architecture.md §87.
    IdempotencyKeys,
    /// docs/architecture.md §163.
    ResolvedDeadlines,
}

impl RetentionTarget {
    fn task(self) -> &'static str {
        match self {
            RetentionTarget::IdempotencyKeys => "idempotency_key_retention",
            RetentionTarget::ResolvedDeadlines => "deadline_retention",
        }
    }

    async fn delete_batch(
        self,
        pool: &Pool,
        bounded_context: &str,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> skilj_core::error::Result<u64> {
        match self {
            // External events' message keys share the retention
            // (docs/architecture.md §175). The two counts are summed, so a
            // batch is "full" while either table still filled its own.
            RetentionTarget::IdempotencyKeys => {
                let commands = skilj_core::db::delete_expired_idempotency_keys(
                    pool,
                    bounded_context,
                    cutoff,
                    IDEMPOTENCY_KEY_CLEANUP_BATCH,
                )
                .await?;
                let external = skilj_core::db::delete_expired_external_message_keys(
                    pool,
                    bounded_context,
                    cutoff,
                    IDEMPOTENCY_KEY_CLEANUP_BATCH,
                )
                .await?;
                Ok(commands + external)
            }
            RetentionTarget::ResolvedDeadlines => {
                skilj_core::db::delete_expired_deadlines(
                    pool,
                    bounded_context,
                    cutoff,
                    IDEMPOTENCY_KEY_CLEANUP_BATCH,
                )
                .await
            }
        }
    }
}

/// One pass of a retention task: in every bounded context, delete what
/// `target` names recorded (or resolved) more than `retention` ago, a
/// bounded batch at a time.
async fn retention_tick(pool: &Pool, target: RetentionTarget, retention: chrono::Duration) {
    let task = target.task();
    let cutoff = chrono::Utc::now()
        .checked_sub_signed(retention)
        .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);
    let bounded_contexts = match skilj_core::db::list_bounded_contexts(pool).await {
        Ok(bcs) => bcs,
        Err(e) => {
            tracing::warn!(error = %e, task, "retention failed to list bounded contexts");
            BACKGROUND_TASK_ERRORS.add(
                1,
                &[
                    KeyValue::new("task", task),
                    KeyValue::new("reason", "list_bounded_contexts_failed"),
                ],
            );
            return;
        }
    };
    stream::iter(&bounded_contexts)
        .for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, |bc| {
            contain_panic(task, bc.name.clone(), async move {
                loop {
                    match target.delete_batch(pool, &bc.name, cutoff).await {
                        Ok(deleted) if deleted < IDEMPOTENCY_KEY_CLEANUP_BATCH as u64 => break,
                        Ok(_) => continue,
                        Err(e) => {
                            tracing::warn!(
                                bounded_context = %bc.name,
                                error = %e,
                                task,
                                "retention failed"
                            );
                            BACKGROUND_TASK_ERRORS.add(
                                1,
                                &[
                                    KeyValue::new("task", task),
                                    KeyValue::new("reason", "delete_failed"),
                                ],
                            );
                            break;
                        }
                    }
                }
            })
        })
        .await;
}

/// Codeberg issue #20's own firing half - not tied to any one
/// registered `ScheduleDeadline` the way `catch_up_schedule_deadline`/
/// `catch_up_cancel_deadline` each are (see `db::fire_due_deadlines`'s
/// own doc comment for why), so this fans out over every *currently
/// active* bounded context each tick instead - the identical shape
/// `scheduler_tick` below already uses for its own per-bc due-occurrence
/// scan.
#[allow(clippy::too_many_arguments)]
async fn deadline_fire_tick(
    pool: &Pool,
    command_dispatcher: &dyn CommandDispatcher,
    projection_dispatcher: &dyn skilj_core::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn skilj_core::plugin::SnapshotDispatcher,
    broadcaster: &EventBroadcaster,
    event_cache: &EventCache,
    encryption_master_key: Option<&EncryptionMasterKey>,
    now: chrono::DateTime<chrono::Utc>,
    retry_policy: &skilj_retry::RetryPolicy,
    cancels: &[skilj_core::plugin::CancelDeadlineInfo],
) {
    let bounded_contexts = match skilj_core::db::list_bounded_contexts(pool).await {
        Ok(bcs) => bcs,
        Err(e) => {
            tracing::warn!(error = %e, "deadline fire tick failed to list bounded contexts");
            BACKGROUND_TASK_ERRORS.add(
                1,
                &[
                    KeyValue::new("task", "deadline_fire"),
                    KeyValue::new("reason", "list_bounded_contexts_failed"),
                ],
            );
            return;
        }
    };
    stream::iter(&bounded_contexts)
        .for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, |bc| {
            contain_panic("deadline_fire", bc.name.clone(), async move {
                if bc.status != skilj_core::event_store::BoundedContextStatus::Active {
                    return;
                }
                if let Err(e) = skilj_core::db::fire_due_deadlines(
                    pool,
                    command_dispatcher,
                    projection_dispatcher,
                    snapshot_dispatcher,
                    broadcaster,
                    event_cache,
                    &bc.name,
                    now,
                    encryption_master_key,
                    retry_policy,
                    cancels,
                )
                .await
                {
                    tracing::warn!(
                        bounded_context = %bc.name,
                        error = %e,
                        "deadline fire tick failed"
                    );
                    BACKGROUND_TASK_ERRORS.add(
                        1,
                        &[
                            KeyValue::new("task", "deadline_fire"),
                            KeyValue::new("reason", "fire_due_deadlines_failed"),
                        ],
                    );
                }
            })
        })
        .await;
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
/// `schedule_position`.
///
/// `fire_once` doesn't walk at all: it raises only the latest occurrence
/// already due (`event_store::latest_occurrence_at_or_before`), the one
/// its backlog collapses into - every earlier one would be rejected, and
/// the position doesn't move until that last one fires. Walking to it
/// from the position used to be how it was reached, which never arrived
/// once the backlog outgrew `MAX_OCCURRENCES_PER_TICK`
/// (docs/architecture.md §150); raising only the earliest, before that,
/// never arrived at all.
///
/// Each occurrence raised for `skip`/`replay_backlog` is handled per
/// policy:
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
///     occurrence in turn, moving the position with each, so a backlog
///     longer than one tick's walk is simply continued next tick.
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
            contain_panic(
                "scheduler",
                bc.name.clone(),
                scheduler_tick_for_bounded_context(
                    pool,
                    projection_dispatcher,
                    event_dispatcher,
                    broadcaster,
                    event_cache,
                    encryption_master_key,
                    now,
                    bc,
                ),
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

        // `fire_once` fires only a backlog's last occurrence, and nothing
        // moves the position until it does - so walking up to it from the
        // position, as below, never arrived once the backlog was longer
        // than one tick's walk: every tick restarted from the same place
        // (docs/architecture.md §150). It's raised directly instead; the
        // occurrences before it are the ones `create_system_event` would
        // have rejected anyway.
        if policy == skilj_core::event_store::MissedOccurrencePolicy::FireOnce {
            let Some(last_due) =
                skilj_core::event_store::latest_occurrence_at_or_before(schedule, now)
                    .filter(|last| *last > initial_position)
            else {
                continue;
            };
            if let Err(e) = skilj_core::db::fire_system_event(
                pool,
                projection_dispatcher,
                event_dispatcher,
                broadcaster,
                event_cache,
                &bc.name,
                &et.name,
                last_due,
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
            }
            continue;
        }

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
    version: &RegistrationVersion,
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
        if existing.is_some()
            && version
                .is_older_than_stored(
                    pool,
                    bounded_context_name,
                    skilj_core::db::RegistrationTable::EventTypes,
                    name,
                )
                .await?
        {
            tracing::warn!(
                event_type = %key,
                "a newer application version registered this EventType - keeping its registration"
            );
            report.kept_newer.push(key);
            continue;
        }
        let (sensitive_fields, private_fields, owner_tag_key, kept_protections) = match &existing {
            Some(stored) => keep_stored_protections(
                &registered.sensitive_fields,
                &registered.private_fields,
                &registered.owner_tag_key,
                &stored.sensitive_fields,
                &stored.private_fields,
                &stored.owner_tag_key,
            ),
            None => (
                registered.sensitive_fields.clone(),
                registered.private_fields.clone(),
                registered.owner_tag_key.clone(),
                false,
            ),
        };
        let register = |existing: Option<&skilj_core::event_store::EventType>| {
            skilj_core::event_store::register_event_type(
                &mapping,
                &bc,
                name.clone(),
                registered.schema.clone(),
                registered.tag_mappings.clone(),
                owner_tag_key.clone(),
                sensitive_fields.clone(),
                private_fields.clone(),
                registered.external_creation_allowed,
                registered.direct_creation_allowed,
                registered.system_triggered_allowed,
                registered.system_triggered_schedule.clone(),
                registered.missed_occurrence_policy,
                registered.event_read_allowed,
                existing,
                now,
            )
        };
        let registration = match (register(existing.as_ref()), existing.as_ref()) {
            (Ok(registration), _) => registration,
            // docs/architecture.md §101: refused as a narrowing - but if the
            // stored registration is itself a valid evolution of this
            // process's, this process is simply older. Keep the newer one.
            (Err(e), Some(stored)) if is_older_than_stored_rejection(&e) => {
                let ours = register(None)?;
                let stored_evolves_ours = skilj_core::event_store::register_event_type(
                    &mapping,
                    &bc,
                    name.clone(),
                    stored.schema.clone(),
                    stored.tag_mappings.clone(),
                    stored.owner_tag_key.clone(),
                    stored.sensitive_fields.clone(),
                    stored.private_fields.clone(),
                    stored.external_creation_allowed,
                    stored.direct_creation_allowed,
                    stored.system_triggered_allowed,
                    stored.system_triggered_schedule.clone(),
                    stored.missed_occurrence_policy,
                    stored.event_read_allowed,
                    Some(ours.event_type()),
                    now,
                )
                .is_ok();
                if !stored_evolves_ours {
                    return Err(e);
                }
                tracing::warn!(
                    event_type = %key,
                    "this process declares an older shape of this EventType than the stored one \
                     (a newer version registered it) - keeping the stored registration"
                );
                report.kept_newer.push(key);
                continue;
            }
            (Err(e), _) => return Err(e),
        };
        if kept_protections {
            tracing::warn!(
                event_type = %key,
                "this process's EventType declares fewer protections (sensitive/private \
                 fields, owner tag key) than the stored registration - keeping the stored ones"
            );
            report.kept_protections.push(key.clone());
        }
        skilj_core::db::upsert_event_type(pool, registration.event_type()).await?;
        version
            .stamp(
                pool,
                bounded_context_name,
                skilj_core::db::RegistrationTable::EventTypes,
                name,
            )
            .await?;
        report.registered.push(key);
    }
    Ok(())
}

async fn reconcile_command_types(
    pool: &Pool,
    role: &Role,
    command_types: &HashMap<(String, String), RegisteredCommandType>,
    report: &mut ReconciliationReport,
    version: &RegistrationVersion,
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
        if existing.is_some()
            && version
                .is_older_than_stored(
                    pool,
                    bounded_context_name,
                    skilj_core::db::RegistrationTable::CommandTypes,
                    name,
                )
                .await?
        {
            tracing::warn!(
                command_type = %key,
                "a newer application version registered this CommandType - keeping its registration"
            );
            report.kept_newer.push(key);
            continue;
        }
        let (sensitive_fields, private_fields, owner_tag_key, kept_protections) = match &existing {
            Some(stored) => keep_stored_protections(
                &registered.sensitive_fields,
                &registered.private_fields,
                &registered.owner_tag_key,
                &stored.sensitive_fields,
                &stored.private_fields,
                &stored.owner_tag_key,
            ),
            None => (
                registered.sensitive_fields.clone(),
                registered.private_fields.clone(),
                registered.owner_tag_key.clone(),
                false,
            ),
        };
        let register = |existing: Option<&skilj_core::event_store::CommandType>| {
            skilj_core::event_store::register_command_type(
                &mapping,
                &bc,
                name.clone(),
                registered.schema.clone(),
                registered.tag_mappings.clone(),
                owner_tag_key.clone(),
                sensitive_fields.clone(),
                private_fields.clone(),
                registered.rest_trigger_allowed,
                existing,
            )
        };
        let registration = match (register(existing.as_ref()), existing.as_ref()) {
            (Ok(registration), _) => registration,
            // docs/architecture.md §101 - see `reconcile_event_types`.
            (Err(e), Some(stored)) if is_older_than_stored_rejection(&e) => {
                let ours = register(None)?;
                let stored_evolves_ours = skilj_core::event_store::register_command_type(
                    &mapping,
                    &bc,
                    name.clone(),
                    stored.schema.clone(),
                    stored.tag_mappings.clone(),
                    stored.owner_tag_key.clone(),
                    stored.sensitive_fields.clone(),
                    stored.private_fields.clone(),
                    stored.rest_trigger_allowed,
                    Some(ours.command_type()),
                )
                .is_ok();
                if !stored_evolves_ours {
                    return Err(e);
                }
                tracing::warn!(
                    command_type = %key,
                    "this process declares an older shape of this CommandType than the stored \
                     one (a newer version registered it) - keeping the stored registration"
                );
                report.kept_newer.push(key);
                continue;
            }
            (Err(e), _) => return Err(e),
        };
        if kept_protections {
            tracing::warn!(
                command_type = %key,
                "this process's CommandType declares fewer protections (sensitive/private \
                 fields, owner tag key) than the stored registration - keeping the stored ones"
            );
            report.kept_protections.push(key.clone());
        }
        skilj_core::db::upsert_command_type(pool, registration.command_type()).await?;
        version
            .stamp(
                pool,
                bounded_context_name,
                skilj_core::db::RegistrationTable::CommandTypes,
                name,
            )
            .await?;
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
    version: &RegistrationVersion,
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
        if existing.is_some()
            && version
                .is_older_than_stored(
                    pool,
                    bounded_context_name,
                    skilj_core::db::RegistrationTable::Projections,
                    name,
                )
                .await?
        {
            tracing::warn!(
                projection = %key,
                "a newer application version registered this Projection - keeping its registration"
            );
            report.kept_newer.push(key);
            continue;
        }
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
        let bounded_context_events = skilj_core::db::witness_events_of_types(
            pool,
            bounded_context_name,
            &consumed_event_types,
        )
        .await?;

        let registration = match skilj_core::projections::register_projection(
            &mapping,
            &bc,
            name.clone(),
            registered.schema.clone(),
            consumed_event_types,
            registered.sync,
            existing.as_ref(),
            staged.as_ref(),
            &bounded_context_events,
        ) {
            Ok(registration) => registration,
            // docs/architecture.md §103: §101 for projections - a newer
            // version added a state field this process doesn't declare.
            // When the stored state schema is a compatible evolution of
            // ours, this process is older: keep the stored projection as
            // it is (no rebuild staged back to the older shape).
            Err(e)
                if is_older_than_stored_rejection(&e)
                    && existing.as_ref().is_some_and(|stored| {
                        skilj_core::event_store::schema_is_backwards_compatible(
                            &registered.schema,
                            &stored.schema,
                        )
                    }) =>
            {
                tracing::warn!(
                    projection = %key,
                    "this process declares an older state schema for this Projection than the \
                     stored one (a newer version registered it) - keeping the stored projection"
                );
                report.kept_newer.push(key);
                continue;
            }
            Err(e) => return Err(e),
        };
        match registration {
            ProjectionRegistration::Created {
                projection,
                needs_history_fold,
            } => {
                // No `projection_state` seeding here (§9's "keyed /
                // multi-row Projections" pass) - instances are created
                // lazily, on first touch. `needs_history_fold` (drift
                // audit finding #3) is the one exception: a first-time
                // sync projection with pre-existing matching history is
                // folded right here, in a way no concurrent write can
                // interleave with (docs/architecture.md §113).
                skilj_core::db::create_projection(
                    pool,
                    &projection,
                    needs_history_fold,
                    dispatcher,
                )
                .await?;
            }
            ProjectionRegistration::ReconciledTrivially(projection) => {
                skilj_core::db::upsert_projection(pool, &projection).await?;
            }
            ProjectionRegistration::RebuildStaged(rebuild) => {
                skilj_core::db::upsert_projection_rebuild(pool, &rebuild).await?;
            }
        }
        // Stamped on the live row even when a rebuild was staged, so an
        // older version doesn't stage one back before it's promoted.
        version
            .stamp(
                pool,
                bounded_context_name,
                skilj_core::db::RegistrationTable::Projections,
                name,
            )
            .await?;
        report.registered.push(key);
    }
    Ok(())
}
