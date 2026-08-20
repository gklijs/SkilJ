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

pub use skilj_core::access_control::{IdpConfig, SigningAlgorithm};
pub use skilj_core::encryption::EncryptionMasterKey;
pub use skilj_core::plugin::{requires_role, CommandType, EventType, Projection};

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
}

/// `CommandDispatcher`'s own implementer - a thin wrapper around the
/// registry `Skilj` holds, kept as its own type rather than implementing
/// the trait on `Skilj` directly so `rest_router()` can produce an
/// `Arc<dyn CommandDispatcher>` cheaply (cloning the `Arc<HashMap<...>>`
/// alone) without requiring an `Arc<Skilj>`.
struct Dispatcher {
    command_types: Arc<HashMap<(String, String), RegisteredCommandType>>,
}

impl CommandDispatcher for Dispatcher {
    fn dispatch(
        &self,
        bounded_context: &str,
        command_type: &str,
        payload: &str,
        matching_events: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        let registered = self
            .command_types
            .get(&(bounded_context.to_string(), command_type.to_string()))?;
        Some((registered.decide)(payload, matching_events))
    }

    fn required_role(
        &self,
        bounded_context: &str,
        command_type: &str,
    ) -> Option<Option<&'static str>> {
        let registered = self
            .command_types
            .get(&(bounded_context.to_string(), command_type.to_string()))?;
        Some(registered.required_role)
    }
}

/// `ProjectionDispatcher`'s own implementer - same shape and reasoning
/// as `Dispatcher` above, over the `projections` registry instead of
/// `command_types`.
struct ProjectionDispatcherImpl {
    projections: Arc<HashMap<(String, String), RegisteredProjection>>,
}

impl skilj_core::plugin::ProjectionDispatcher for ProjectionDispatcherImpl {
    fn keys(
        &self,
        bounded_context: &str,
        projection_name: &str,
        event: &Event,
    ) -> Option<Vec<String>> {
        let registered = self
            .projections
            .get(&(bounded_context.to_string(), projection_name.to_string()))?;
        Some((registered.keys)(event))
    }

    fn project(
        &self,
        bounded_context: &str,
        projection_name: &str,
        state_json: &str,
        event: &Event,
        key: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        let registered = self
            .projections
            .get(&(bounded_context.to_string(), projection_name.to_string()))?;
        Some((registered.project)(state_json, event, key))
    }

    fn default_state(&self, bounded_context: &str, projection_name: &str) -> Option<String> {
        let registered = self
            .projections
            .get(&(bounded_context.to_string(), projection_name.to_string()))?;
        Some(registered.default_state_json.clone())
    }
}

/// `EventDispatcher`'s own implementer - same shape and reasoning as
/// `Dispatcher`/`ProjectionDispatcherImpl` above, over the `event_types`
/// registry instead.
struct EventDispatcherImpl {
    event_types: Arc<HashMap<(String, String), RegisteredEventType>>,
}

impl skilj_core::plugin::EventDispatcher for EventDispatcherImpl {
    fn scheduled_payload(&self, bounded_context: &str, event_type: &str) -> Option<String> {
        let registered = self
            .event_types
            .get(&(bounded_context.to_string(), event_type.to_string()))?;
        Some((registered.scheduled_payload)())
    }
}

impl Skilj {
    pub fn builder(database_url: impl Into<String>) -> SkiljBuilder {
        SkiljBuilder {
            database_url: database_url.into(),
            current_bounded_context: None,
            reconciliation_role: None,
            identity_provider: None,
            event_types: HashMap::new(),
            command_types: HashMap::new(),
            projections: HashMap::new(),
            async_projection_poll_interval: std::time::Duration::from_millis(500),
            scheduler_poll_interval: std::time::Duration::from_secs(1),
            projection_query_wait_timeout: std::time::Duration::from_secs(5),
            encryption_master_key: None,
            event_broadcast_capacity: 1024,
            event_cache_warm_up_count: 1000,
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
        })
    }

    /// The type-erased `ProjectionDispatcher` this `Skilj` hands
    /// `rest_router()`/`graphql_router()` internally - `db::
    /// insert_event_and_update_sync_projections`'s own bridge into a
    /// bounded context's registered `project()` implementations (§8
    /// item 6). Same cheap-`Arc`-clone reasoning as `command_dispatcher()`.
    pub fn projection_dispatcher(&self) -> Arc<dyn skilj_core::plugin::ProjectionDispatcher> {
        Arc::new(ProjectionDispatcherImpl {
            projections: self.projections.clone(),
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
    /// docs/architecture.md §7. A caller wanting the REST and GraphQL
    /// surfaces combined merges this with `graphql_router()` however
    /// `axum::Router::merge`/`nest` suits their own application.
    pub fn rest_router(&self) -> axum::Router {
        skilj_rest::router(
            self.pool.clone(),
            self.command_dispatcher(),
            self.projection_dispatcher(),
            self.encryption_master_key.clone(),
            self.event_broadcaster.clone(),
            self.event_cache.clone(),
        )
    }

    /// `skilj-graphql`'s single unified schema, mounted onto a fresh
    /// `axum::Router` - see docs/architecture.md §5. A caller wanting the
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
    /// `async`, returning `Result`: building `ProjectionQuery`'s own
    /// per-projection GraphQL types (§5.1) means `schema::build` now
    /// makes real database calls, a failure mode this signature needs to
    /// carry - see `skilj_graphql::router`'s own doc comment.
    pub async fn graphql_router(&self) -> skilj_core::error::Result<axum::Router> {
        let state = skilj_graphql::GraphqlState {
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
            projection_query_wait_timeout: self.projection_query_wait_timeout,
            encryption_master_key: self.encryption_master_key.clone(),
            event_broadcaster: self.event_broadcaster.clone(),
            revocation_broadcaster: self.revocation_broadcaster.clone(),
            event_cache: self.event_cache.clone(),
        };
        skilj_graphql::router(state).await
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
    sensitive_fields: Vec<skilj_core::shared::SensitiveField>,
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
        sensitive_fields: T::sensitive_fields(),
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
/// (§8 item 4) once per submission: deserialize the payload into
/// `T::Payload`, convert every matching raw `Event` into `T::Event` via
/// `BoundedContextEvent::try_from_event`, then call `T::decide()`. Either
/// decode step failing surfaces as `EventStoreError::PayloadDecodeFailed`,
/// a library-level error rather than a `CommandDecision::Rejected` - see
/// that variant's own doc comment for why.
type DeciderFn =
    Box<dyn Fn(&str, &[Event]) -> skilj_core::error::Result<CommandDecision> + Send + Sync>;

struct RegisteredCommandType {
    schema: String,
    tag_mappings: Vec<skilj_core::shared::TagMapping>,
    sensitive_fields: Vec<skilj_core::shared::SensitiveField>,
    rest_trigger_allowed: bool,
    /// `CommandType::required_role()`'s value, carried straight through
    /// unchanged - not persisted anywhere (see that method's own doc
    /// comment), read back only by `Dispatcher::required_role` for
    /// `skilj-graphql`'s eventual mutation resolver to check (§1.3.1,
    /// §8 item 5).
    required_role: Option<&'static str>,
    /// Called by `Dispatcher::dispatch` (this module's own
    /// `CommandDispatcher` implementer), reached from `skilj-rest`'s
    /// `CommandTrigger` route through the `Arc<dyn CommandDispatcher>`
    /// `rest_router()` hands it - docs/architecture.md §8 item 4.
    decide: DeciderFn,
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
        sensitive_fields: T::sensitive_fields(),
        rest_trigger_allowed: T::rest_trigger_allowed(),
        required_role: T::required_role(),
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
/// (§8 item 6) through the `ProjectionDispatcher` bridge - `Dispatcher`
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
/// lazily, the first time any event touches its key (§9's own "keyed /
/// multi-row Projections" pass - nothing is seeded up front anymore,
/// since a projection's own instances aren't known until events actually
/// name them).
struct RegisteredProjection {
    schema: String,
    consumed_event_types: Vec<&'static str>,
    sync: bool,
    default_state_json: String,
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

pub struct SkiljBuilder {
    database_url: String,
    current_bounded_context: Option<String>,
    reconciliation_role: Option<String>,
    identity_provider: Option<IdpConfig>,
    event_types: HashMap<(String, String), RegisteredEventType>,
    command_types: HashMap<(String, String), RegisteredCommandType>,
    projections: HashMap<(String, String), RegisteredProjection>,
    async_projection_poll_interval: std::time::Duration,
    scheduler_poll_interval: std::time::Duration,
    projection_query_wait_timeout: std::time::Duration,
    encryption_master_key: Option<EncryptionMasterKey>,
    event_broadcast_capacity: usize,
    event_cache_warm_up_count: usize,
}

impl SkiljBuilder {
    /// Every `event_type`/`command_type`/`projection` call following this
    /// one registers against `name`, until the next `bounded_context`
    /// call changes it.
    pub fn bounded_context(mut self, name: impl Into<String>) -> Self {
        self.current_bounded_context = Some(name.into());
        self
    }

    fn current_bounded_context(&self) -> String {
        self.current_bounded_context.clone().expect(
            "SkiljBuilder: .event_type::<T>()/.command_type::<T>()/.projection::<T>() called \
             before .bounded_context(...) - every one of those calls registers against the \
             most recently named bounded context",
        )
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
    /// §6. Optional: omitting it means `graphql_router()` still mounts
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
    /// own wake mechanism (§8 item 6, async case). Defaults to 500ms;
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

    /// Runs the startup reconciliation loop automatically (§1.5). Returns
    /// `Err` only for a genuine registration rejection (e.g. an
    /// incompatible schema change) - a bounded context the reconciliation
    /// Role has no admin access to yet is reported in
    /// `ReconciliationReport`, not an error.
    pub async fn build(self) -> Result<(Skilj, ReconciliationReport), skilj_core::Error> {
        let pool = skilj_core::db::connect(&self.database_url).await?;
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
        // the same `Arc` (cheap to clone) can back both the reconciliation
        // dispatcher below - needed for `needs_history_fold` (drift audit
        // finding #3) - and the field `Skilj` is built with at the bottom
        // of this function.
        let projections = Arc::new(self.projections);

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
            reconcile_command_types(&pool, &role, &self.command_types, &mut report).await?;
            let reconciliation_dispatcher = ProjectionDispatcherImpl {
                projections: projections.clone(),
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
        let event_cache = EventCache::new(self.event_cache_warm_up_count);
        for bc in skilj_core::db::list_bounded_contexts(&pool).await? {
            event_cache.warm(&pool, &bc.name).await?;
        }

        let skilj = Skilj {
            pool,
            command_types: Arc::new(self.command_types),
            projections,
            event_types: Arc::new(self.event_types),
            bootstrap_secret,
            identity_provider,
            projection_query_wait_timeout,
            encryption_master_key,
            event_broadcaster,
            revocation_broadcaster,
            event_cache,
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
                match skilj_core::db::list_bounded_contexts(&poll_pool).await {
                    Ok(bounded_contexts) => {
                        for bc in bounded_contexts {
                            if let Err(e) = skilj_core::db::catch_up_bounded_context(
                                &poll_pool,
                                &bc.name,
                                poll_dispatcher.as_ref(),
                            )
                            .await
                            {
                                eprintln!(
                                    "skilj: async projection catch-up failed for bounded \
                                     context {:?}: {e}",
                                    bc.name
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "skilj: async projection catch-up failed to list bounded \
                             contexts: {e}"
                        );
                    }
                }
                tokio::time::sleep(poll_interval).await;
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
                scheduler_tick(
                    &scheduler_pool,
                    scheduler_projection_dispatcher.as_ref(),
                    scheduler_event_dispatcher.as_ref(),
                    &scheduler_broadcaster,
                    &scheduler_event_cache,
                    scheduler_encryption_master_key.as_ref(),
                    chrono::Utc::now(),
                )
                .await;
                tokio::time::sleep(scheduler_interval).await;
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
            eprintln!("skilj: scheduler failed to list bounded contexts: {e}");
            return;
        }
    };
    for bc in &bounded_contexts {
        if bc.status != skilj_core::event_store::BoundedContextStatus::Active {
            continue;
        }
        let scheduled = match skilj_core::db::list_scheduled_event_types(pool, &bc.name).await {
            Ok(scheduled) => scheduled,
            Err(e) => {
                eprintln!(
                    "skilj: scheduler failed to list scheduled event types for {:?}: {e}",
                    bc.name
                );
                continue;
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
                        eprintln!(
                            "skilj: SkipMissedOccurrences failed for {}/{}: {e}",
                            bc.name, et.name
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
                    eprintln!(
                        "skilj: CreateSystemEvent failed for {}/{}: {e}",
                        bc.name, et.name
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
            registered.sensitive_fields.clone(),
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
            registered.sensitive_fields.clone(),
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
