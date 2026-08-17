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

use skilj_core::access_control::{AccessLevel, JwksCache, Role};
use skilj_core::bootstrap::BootstrapSecret;
use skilj_core::db::Pool;
use skilj_core::event_store::{Error as EventStoreError, Event};
use skilj_core::plugin::{BoundedContextEvent, CommandDispatcher};
use skilj_core::projections::ProjectionRegistration;
use skilj_core::shared::CommandDecision;
use std::collections::HashMap;
use std::sync::Arc;

pub use skilj_core::access_control::{IdpConfig, SigningAlgorithm};
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
    fn project(
        &self,
        bounded_context: &str,
        projection_name: &str,
        state_json: &str,
        event: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        let registered = self
            .projections
            .get(&(bounded_context.to_string(), projection_name.to_string()))?;
        Some((registered.project)(state_json, event))
    }

    fn default_state(&self, bounded_context: &str, projection_name: &str) -> Option<String> {
        let registered = self
            .projections
            .get(&(bounded_context.to_string(), projection_name.to_string()))?;
        Some(registered.default_state_json.clone())
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
    /// surfaces combined merges this with the eventual GraphQL router
    /// (§8 item 5, not built yet) however `axum::Router::merge`/`nest`
    /// suits their own application.
    pub fn rest_router(&self) -> axum::Router {
        skilj_rest::router(
            self.pool.clone(),
            self.command_dispatcher(),
            self.projection_dispatcher(),
        )
    }

    /// `skilj-graphql`'s single unified schema, mounted onto a fresh
    /// `axum::Router` - see docs/architecture.md §5. A caller wanting the
    /// REST and GraphQL surfaces combined merges this with
    /// `rest_router()` however `axum::Router::merge`/`nest` suits their
    /// own application - see `rest_router()`'s own doc comment.
    ///
    /// Phase 1 only (docs/architecture.md §8 item 5's own plan): the
    /// superadmin admin console - `createSuperadmin`, `createRole`/
    /// `revokeRole`/`grantRoleAccessMapping`/`revokeRoleAccessMapping`,
    /// `addBoundedContext`/`archiveBoundedContext`/`deleteBoundedContext`,
    /// `boundedContexts`. The five dynamic, per-bounded-context business
    /// surfaces (`ProjectionQuery`/`EventQuery`/`CommandQuery`/
    /// `CommandSubmission`/`EventSubscription`) are a later phase.
    pub fn graphql_router(&self) -> axum::Router {
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
        };
        skilj_graphql::router(state)
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

/// A `.event_type::<T>()` call's own captured data - everything
/// `register_event_type` needs except the caller-supplied `access_mapping`/
/// `bounded_context`/`existing`, which reconciliation resolves at
/// `.build()` time, not registration time. `system_triggered_allowed`/
/// `system_triggered_schedule` stay hard-defaulted (`false`/`None`) per
/// §1.7 - no scheduler exists yet to consult them.
struct RegisteredEventType {
    schema: String,
    tag_mappings: Vec<skilj_core::shared::TagMapping>,
    sensitive_fields: Vec<skilj_core::shared::SensitiveField>,
    external_creation_allowed: bool,
    direct_creation_allowed: bool,
    event_read_allowed: bool,
}

fn registered_event_type<T: EventType>() -> RegisteredEventType {
    let schema = schemars::schema_for!(T::Payload);
    RegisteredEventType {
        schema: serde_json::to_string(&schema).expect("JSON Schema serialization is infallible"),
        tag_mappings: T::tag_mappings(),
        sensitive_fields: T::sensitive_fields(),
        external_creation_allowed: T::external_creation_allowed(),
        direct_creation_allowed: T::direct_creation_allowed(),
        event_read_allowed: T::event_read_allowed(),
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

/// Applies one event to a projection's current state (as JSON), closing
/// over a single `T: Projection` alone - no runtime captures beyond the
/// `Box` itself, the same shape `DeciderFn` above already has. Built
/// once, at `.projection::<T>()` call time (see `registered_projection`
/// below), called by the new transactional `db::
/// insert_event_and_update_sync_projections` (§8 item 6) through the
/// `ProjectionDispatcher` bridge - `Dispatcher` below, this module's own
/// implementer. Returns `state_json` unchanged when `event`'s own type
/// isn't one this projection actually consumes - see
/// `ProjectionDispatcher::project`'s own doc comment for why that check
/// can't be left to `BoundedContextEvent::try_from_event` alone.
type ProjectFn = Box<dyn Fn(&str, &Event) -> skilj_core::error::Result<String> + Send + Sync>;

/// See `RegisteredEventType` above - same shape and reasoning, for
/// `Projection`. `consumed_event_types` carries `EventType::NAME`s only
/// (`&'static str`s from `Projection::consumed_event_types()`) -
/// resolved into full `event_store::EventType`s during reconciliation,
/// once the bounded context's own admin access has already been
/// confirmed (see `reconcile_projections` below). `default_state_json`
/// is what a fresh `projection_state` row is seeded with at registration
/// time (`T::State::default()`, serialised) - `project`'s own closure
/// never needs to invent a starting point at fold time.
struct RegisteredProjection {
    schema: String,
    consumed_event_types: Vec<&'static str>,
    sync: bool,
    default_state_json: String,
    project: ProjectFn,
}

fn registered_projection<T: Projection + 'static>() -> RegisteredProjection {
    let schema = schemars::schema_for!(T::State);
    let consumed_event_types = T::consumed_event_types();
    let consumed_for_closure = consumed_event_types.clone();
    let default_state_json = serde_json::to_string(&T::State::default())
        .expect("JSON serialization of a Default::default() State is infallible");
    RegisteredProjection {
        schema: serde_json::to_string(&schema).expect("JSON Schema serialization is infallible"),
        consumed_event_types,
        sync: T::sync(),
        default_state_json,
        project: Box::new(move |state_json, event| {
            if !consumed_for_closure
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
            T::project(&mut state, &converted);
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

    pub fn event_type<T: EventType>(mut self) -> Self {
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

    /// Runs the startup reconciliation loop automatically (§1.5). Returns
    /// `Err` only for a genuine registration rejection (e.g. an
    /// incompatible schema change) - a bounded context the reconciliation
    /// Role has no admin access to yet is reported in
    /// `ReconciliationReport`, not an error.
    pub async fn build(self) -> Result<(Skilj, ReconciliationReport), skilj_core::Error> {
        let pool = skilj_core::db::connect(&self.database_url).await?;
        skilj_core::db::migrate(&pool).await?;

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

        let mut report = ReconciliationReport::default();
        if let Some(external_subject) = &self.reconciliation_role {
            let role = skilj_core::access_control::resolve_role_by_external_subject(
                external_subject,
                &roles,
            )?
            .clone();

            reconcile_event_types(&pool, &role, &self.event_types, &mut report).await?;
            reconcile_command_types(&pool, &role, &self.command_types, &mut report).await?;
            reconcile_projections(&pool, &role, &self.projections, &mut report).await?;
        }

        let identity_provider = self.identity_provider.map(|config| {
            Arc::new(IdentityProvider {
                cache: Arc::new(JwksCache::new(config.jwks_endpoint.clone())),
                config,
            })
        });

        let poll_interval = self.async_projection_poll_interval;
        let skilj = Skilj {
            pool,
            command_types: Arc::new(self.command_types),
            projections: Arc::new(self.projections),
            bootstrap_secret,
            identity_provider,
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

        Ok((skilj, report))
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

async fn reconcile_event_types(
    pool: &Pool,
    role: &Role,
    event_types: &HashMap<(String, String), RegisteredEventType>,
    report: &mut ReconciliationReport,
) -> Result<(), skilj_core::Error> {
    for ((bounded_context_name, name), registered) in event_types {
        let key = format!("{bounded_context_name}/{name}");
        let Some(bc) = skilj_core::db::get_bounded_context(pool, bounded_context_name).await?
        else {
            report.skipped_no_access.push(key);
            continue;
        };
        let Some(mapping) = active_admin_mapping(pool, &role.id, bounded_context_name).await?
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
            false,
            None,
            registered.event_read_allowed,
            existing.as_ref(),
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
        let Some(bc) = skilj_core::db::get_bounded_context(pool, bounded_context_name).await?
        else {
            report.skipped_no_access.push(key);
            continue;
        };
        let Some(mapping) = active_admin_mapping(pool, &role.id, bounded_context_name).await?
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
) -> Result<(), skilj_core::Error> {
    for ((bounded_context_name, name), registered) in projections {
        let key = format!("{bounded_context_name}/{name}");
        let Some(bc) = skilj_core::db::get_bounded_context(pool, bounded_context_name).await?
        else {
            report.skipped_no_access.push(key);
            continue;
        };
        let Some(mapping) = active_admin_mapping(pool, &role.id, bounded_context_name).await?
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
        let staged =
            skilj_core::db::get_projection_rebuild(pool, bounded_context_name, name).await?;
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
            ProjectionRegistration::Created(projection)
            | ProjectionRegistration::ReconciledTrivially(projection) => {
                skilj_core::db::upsert_projection(pool, &projection).await?;
                // Idempotent - only actually inserts the first time this
                // projection is ever registered (§8 item 6).
                skilj_core::db::seed_projection_state(
                    pool,
                    bounded_context_name,
                    name,
                    &registered.default_state_json,
                )
                .await?;
            }
            ProjectionRegistration::RebuildStaged(rebuild) => {
                skilj_core::db::upsert_projection_rebuild(pool, &rebuild).await?;
            }
        }
        report.registered.push(key);
    }
    Ok(())
}
