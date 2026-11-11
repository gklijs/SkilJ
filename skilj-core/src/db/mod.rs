//! sqlx queries and migrations - persistence for every module in this
//! crate. See docs/architecture.md §2.2 for why `sqlx`, and §2.2.1 for
//! why every `Integer`-typed column is `BIGINT`, not `INT`.
//!
//! Every rule in `access_control`/`event_store` is a pure function that
//! takes already-loaded state as parameters (no I/O of its own - §1.1's
//! `decide()`/`project()` split, extended to this crate's own rules) -
//! this module is where that state actually gets loaded from and written
//! back to Postgres. Uses `sqlx::query`/`query_as` (the runtime-checked
//! API), not the `query!`/`query_as!` macros - this workspace has no live
//! database to check them against at compile time, and a library
//! embedding its own migrations for a consuming application to run
//! shouldn't assume one exists at `cargo build` time either.
//!
//! **Two-tier schema** (docs/architecture.md §2.2.2): `roles`,
//! `bounded_contexts`, `role_access_mappings` and `access_token_index`
//! live in the global (`public`) schema, tracked by the static
//! `sqlx::migrate!` set in `migrations/`. Everything scoped to one
//! bounded context - event/command types, events, commands, projections,
//! access tokens, read cursors, the sequence counter - lives in that
//! context's own Postgres schema (`bc_<name>`) instead, provisioned
//! dynamically by `provision_bounded_context_schema` when the context is
//! created and torn down in one statement by `hard_delete_bounded_context`,
//! never tracked by `sqlx::migrate!`. This is why most functions below
//! take `bounded_context: &str` as a schema selector, not a `WHERE`
//! filter value: a per-context table's own rows never carry a
//! `bounded_context` column at all, since the schema they live in already
//! says which context they belong to.
//!
//! Scoped, for now, to what's actually wired or registerable - see
//! `migrations/0001_init.sql`'s own doc comment for the full list of
//! what's deliberately not here yet (`EncryptionKey`).

mod master_key;
pub use master_key::{
    assign_encryption_key, bounded_context_key, check_data_key_wraps,
    contexts_with_data_keys_below, data_keys_behind, delete_unused_key, encryption_key_assignments,
    forget_assignments_of_deleted_contexts, key_in_use, mark_data_keys_behind,
    mark_data_keys_rewrapped, master_key_state, master_key_states, register_known_keys,
    register_master_key_event_types, retire_master_key_versions, rewrap_data_keys,
    rewrap_data_keys_once, rotate_master_key, schedule_key_deletion, DataKeyWraps, MasterKeyState,
    RewrapOutcome, Rotation,
};

use crate::access_control::{
    AccessLevel, CommandToken, DirectCreationToken, EventReadStartPosition, EventReadToken,
    ExternalEventToken, PrivateFieldGrant, Role, RoleAccessMapping, RoleStatus, TokenStatus,
};
use crate::access_trail::{fold_access_events, AccessChange, AccessEventKind, AccessEventRecord};
use crate::approvals::{CommandApprovalPolicy, CommandProposal, CommandProposalStatus};
use crate::bootstrap::ContextCreator;
use crate::encryption::{self, DataKey, KeyService};
use crate::event_store::{
    AckMode, BoundedContext, BoundedContextStatus, Command, CommandType, CursorUpdate,
    EncryptionKey, EncryptionKeyStatus, Event, EventOrigin, EventType, MissedOccurrencePolicy,
    ReadCursor, WouldBeEvent,
};
use crate::projections::{Projection, ProjectionRebuild, ProjectionRebuildStatus};
use crate::secrets::{self, Secret, SecretGrant, SecretGrantStatus, SecretStatus, SecretValue};
use crate::shared::{Metadata, PrivateField, QueryItem, SensitiveField, Tag, TagMapping};
use chrono::{DateTime, Utc};
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::KeyValue;
use sqlx::types::Json;
use sqlx::{Acquire, Postgres, Transaction};
use std::sync::LazyLock;

/// This crate's own OTel instrumentation scope. `opentelemetry::global::meter(...)`
/// **snapshot-binds** to whichever `MeterProvider` is globally registered
/// at the moment it's first called (its own doc comment says so
/// explicitly) - unlike the `tracing`/`tracing-opentelemetry` bridge,
/// which is wired to a concrete provider once, inside the consuming
/// app's own `init_telemetry`. Every instrument below is a `LazyLock`,
/// so this is only ever called the first time a metric is actually
/// recorded (the first command processed, the first event appended) -
/// always strictly after a real consuming app's `main()` has already
/// called `opentelemetry::global::set_meter_provider(...)`, since that
/// has to happen before `.build()` even runs. If a future change ever
/// moves telemetry init to *after* `Skilj::builder(...).build()`, this
/// silently goes back to recording into a no-op meter forever - see
/// docs/architecture.md's tracing section.
fn meter() -> &'static Meter {
    static METER: LazyLock<Meter> = LazyLock::new(|| opentelemetry::global::meter("skilj-core"));
    &METER
}

static COMMANDS_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter()
        .u64_counter("skilj.commands.processed")
        .with_description("Commands processed by ProcessCommand, by outcome.")
        .build()
});

static EVENTS_APPENDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    meter()
        .u64_counter("skilj.events.appended")
        .with_description("Events appended to the event store.")
        .build()
});

/// How many commands `CommandBatcher::run_as_leader` actually coalesced
/// into one `commit_command_batch` lock acquisition - the one number
/// none of the `docs/load-test-report-2026-09-18*.md` passes ever
/// measured (all three skipped OTel/Grafana), even though it's exactly
/// what would confirm or refute their shared "batch amortisation is
/// maxed out, per-command work now dominates" reading of the throughput
/// numbers. Recorded once per `commit_command_batch` call, by
/// `bounded_context` - a distribution consistently near 1 under real
/// concurrent load would mean the self-tuning batcher isn't actually
/// forming large batches, a very different diagnosis than "batches are
/// large but per-command work inside them is the bottleneck".
static COMMAND_BATCH_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    meter()
        .u64_histogram("skilj.command_batch.size")
        .with_description("Commands coalesced into one command-batch lock acquisition.")
        .build()
});

/// Called at each of this module's five `broadcaster.publish(event)` call
/// sites, never inside `insert_event` itself - `insert_event` runs inside
/// a still-open transaction its own caller might yet roll back (a later
/// step in the same transaction failing), while `publish` is only ever
/// reached after `tx.commit()` succeeds (see e.g.
/// `insert_event_and_update_sync_projections`'s own doc comment: "after
/// the commit, not before"). Counting at `insert_event` itself would
/// over-count on any such rollback.
fn record_event_appended(event: &Event) {
    EVENTS_APPENDED.add(
        1,
        &[
            KeyValue::new("bounded_context", event.bounded_context.name.clone()),
            KeyValue::new("event_type", event.event_type.name.clone()),
        ],
    );
}

/// `crate::cross_instance`'s sending half for the `skilj_events` channel -
/// called alongside `record_event_appended`, at the same five call sites,
/// for the identical "only after the commit" reason. Every *other*
/// listening instance fetches the real event and republishes it into its
/// own local `EventBroadcaster` - see `crate::cross_instance`'s own
/// module doc comment for the full design. `origin_instance_id` is
/// `EventBroadcaster::instance_id`'s own value at this call site (the
/// broadcaster `publish` was already just called on, immediately above
/// every one of these five call sites) - carried in the payload purely
/// so this same instance's own `NOTIFY` echoing back to itself can be
/// recognised and skipped rather than republished into that identical
/// broadcaster a second time (see `EventBroadcaster::instance_id`'s own
/// doc comment for why that duplication would otherwise happen). A
/// `NOTIFY` failure is logged and swallowed, never propagated: the event
/// it accompanies already committed, and this channel is a pure
/// liveliness signal - a missed notification self-heals via the same
/// DB-backed catch-up path a same-process lagged subscriber already
/// takes, per `@guarantee DeliverySpansInstances` in specs/skilj.allium.
/// [`notify_event_appended`] for many events in one statement - one
/// round trip for a whole command batch instead of one per event, sent
/// before the batch's callers get their answers (docs/architecture.md
/// §196). Each notification carries the same payload as one sent alone.
async fn notify_events_appended(pool: &Pool, events: &[&Event], origin_instance_id: &str) {
    match events {
        [] => return,
        [event] => return notify_event_appended(pool, event, origin_instance_id).await,
        _ => {}
    }
    let payloads: Vec<String> = events
        .iter()
        .map(|event| {
            serde_json::json!({
                "bounded_context": event.bounded_context.name,
                "sequence": event.sequence,
                "origin_instance_id": origin_instance_id,
            })
            .to_string()
        })
        .collect();
    if let Err(err) =
        sqlx::query("SELECT pg_notify('skilj_events', payload) FROM unnest($1::text[]) AS payload")
            .bind(&payloads)
            .execute(pool)
            .await
    {
        tracing::warn!(
            error = %err,
            "NOTIFY skilj_events failed - other instances may miss these events' live push \
             until their next poll-based read"
        );
    }
}

async fn notify_event_appended(pool: &Pool, event: &Event, origin_instance_id: &str) {
    let payload = serde_json::json!({
        "bounded_context": event.bounded_context.name,
        "sequence": event.sequence,
        "origin_instance_id": origin_instance_id,
    })
    .to_string();
    if let Err(err) = sqlx::query("SELECT pg_notify('skilj_events', $1)")
        .bind(payload)
        .execute(pool)
        .await
    {
        tracing::warn!(
            error = %err,
            "NOTIFY skilj_events failed - other instances may miss this event's live push \
             until their next poll-based read"
        );
    }
}

/// `crate::cross_instance`'s sending half for the `skilj_registration_changed`
/// channel - called from every DB-layer function that changes what the
/// GraphQL schema needs to expose (`upsert_event_type`/`upsert_command_type`/
/// `upsert_projection`/`register_new_sync_projection`/
/// `promote_projection_rebuild`/`insert_bounded_context`/
/// `update_bounded_context_status`/`hard_delete_bounded_context`). No payload: which exact type or bounded
/// context changed doesn't matter, every listener reacts identically (a
/// full schema rebuild) - see `crate::cross_instance`'s own module doc
/// comment. Same "log and swallow, never propagate" treatment as
/// `notify_event_appended` and for the identical reason.
async fn notify_registration_changed(pool: &Pool) {
    if let Err(err) = sqlx::query("SELECT pg_notify('skilj_registration_changed', '')")
        .execute(pool)
        .await
    {
        tracing::warn!(
            error = %err,
            "NOTIFY skilj_registration_changed failed - other instances' GraphQL schema may \
             lag until their next registration change or restart"
        );
    }
}

/// `crate::cross_instance`'s sending half for the `skilj_revocations`
/// channel - `skilj-graphql`'s `access_management` resolvers are the only
/// two callers (alongside their own `RevocationBroadcaster::publish`),
/// hence `pub`: this is the one `notify_*` helper reached from outside
/// this module, since revocation, unlike event/registration writes, is
/// driven entirely from the GraphQL layer with no `db::` choke point of
/// its own to hook. `origin_instance_id` gets the identical
/// self-NOTIFY-dedup treatment `notify_event_appended`'s own doc comment
/// describes, here against `RevocationBroadcaster::instance_id`. Same
/// "log and swallow" treatment as its siblings.
pub async fn notify_revocation(
    pool: &Pool,
    mapping: &crate::access_control::RevokedMapping,
    origin_instance_id: &str,
) {
    let payload = serde_json::json!({
        "role_id": mapping.role_id,
        "bounded_context": mapping.bounded_context,
        "origin_instance_id": origin_instance_id,
    })
    .to_string();
    if let Err(err) = sqlx::query("SELECT pg_notify('skilj_revocations', $1)")
        .bind(payload)
        .execute(pool)
        .await
    {
        tracing::warn!(
            error = %err,
            "NOTIFY skilj_revocations failed - other instances may miss this revocation's live \
             push until their next direct database re-check"
        );
    }
}

/// An opaque handle to the connection pool - re-exported so `skilj-rest`/
/// `skilj-graphql` can hold one without depending on `sqlx` directly
/// themselves, the same crate-boundary reasoning docs/architecture.md
/// §3.1 gives for keeping `skilj-core` the only crate that owns the
/// database driver.
pub type Pool = sqlx::PgPool;

/// Re-exported for the identical reason `Pool` is - `SkiljBuilder::
/// pool_options` (skilj's own builder) accepts one of these without
/// `skilj` itself depending on `sqlx` directly.
pub use sqlx::postgres::PgPoolOptions;

/// `sqlx`'s own bare default (`PgPoolOptions::new()`, currently a
/// 10-connection cap with no configured timeouts) - unchanged behaviour
/// for every existing caller. Callers that need to tune pool sizing for
/// real production load (background pollers - async-projection,
/// snapshot, scheduler - already compete with foreground requests for
/// whatever this pool provides) use [`connect_with`] instead, the same
/// "sensible default, escape hatch alongside it" register every other
/// `SkiljBuilder` tunable (`async_projection_poll_interval`,
/// `event_broadcast_capacity`, ...) already uses.
#[tracing::instrument(skip_all)]
pub async fn connect(database_url: &str) -> Result<Pool, sqlx::Error> {
    connect_with(database_url, PgPoolOptions::new()).await
}

/// [`connect`] with caller-supplied pool options (`max_connections`,
/// `min_connections`, `acquire_timeout`, `idle_timeout`, ...) - see that
/// function's own doc comment for why this exists alongside it.
#[tracing::instrument(skip_all)]
pub async fn connect_with(database_url: &str, options: PgPoolOptions) -> Result<Pool, sqlx::Error> {
    connect_with_statement_cache(database_url, options, None).await
}

/// [`connect_with`], with `statement_cache_capacity` (when `Some`)
/// overriding both the database URL's `statement-cache-capacity`
/// parameter and sqlx's default (docs/architecture.md §193).
#[tracing::instrument(skip_all)]
pub async fn connect_with_statement_cache(
    database_url: &str,
    options: PgPoolOptions,
    statement_cache_capacity: Option<usize>,
) -> Result<Pool, sqlx::Error> {
    let mut connect_options: sqlx::postgres::PgConnectOptions = database_url.parse()?;
    if let Some(capacity) = statement_cache_capacity {
        connect_options = connect_options.statement_cache_capacity(capacity);
    }
    options.connect_with(connect_options).await
}

/// sqlx's own default for how many prepared statements each connection
/// keeps (least recently used out).
pub const DEFAULT_STATEMENT_CACHE_CAPACITY: usize = 100;

/// The statement cache capacity a pool connected with
/// [`connect_with_statement_cache`] ends up with: `explicit`, else the
/// database URL's last `statement-cache-capacity` parameter, else
/// [`DEFAULT_STATEMENT_CACHE_CAPACITY`]. sqlx has no getter for it.
pub fn effective_statement_cache_capacity(database_url: &str, explicit: Option<usize>) -> usize {
    explicit
        .or_else(|| {
            let (_, query) = database_url.split_once('?')?;
            // sqlx applies the parameters in order, so a repeated one's
            // last value is what the pool gets.
            query.rsplit('&').find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                (key == "statement-cache-capacity")
                    .then(|| value.parse().ok())
                    .flatten()
            })
        })
        .unwrap_or(DEFAULT_STATEMENT_CACHE_CAPACITY)
}

/// Statements a bounded context needs cached, at least: the command path
/// alone sends 14-20 distinct statements per bounded context, and
/// throughput fell 20-40% once the cache held fewer than about 10 per
/// busy one (docs/architecture.md §193).
pub const STATEMENT_CACHE_MIN_PER_BOUNDED_CONTEXT: usize = 10;

/// Runs every embedded migration under `skilj-core/migrations/` - see
/// this module's own doc comment and docs/architecture.md §2.2 for why
/// `skilj-core` owns its schema this way. Only ever touches the global
/// tables - per-bounded-context schemas are provisioned separately, see
/// `provision_bounded_context_schema`.
#[tracing::instrument(skip_all)]
pub async fn migrate(pool: &Pool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}

mod schema;
pub use schema::*;

mod roles;
pub use roles::*;

mod contexts;
pub use contexts::*;

mod event_types;
pub use event_types::*;

mod command_types;
pub use command_types::*;

mod approvals;
pub use approvals::*;

mod encryption_keys;
pub use encryption_keys::*;

mod secret_records;
pub use secret_records::*;

mod command_records;
pub use command_records::*;

mod projection_records;
pub use projection_records::*;

mod access;
pub use access::*;

mod authorized_events;
pub use authorized_events::*;

mod events;
pub use events::*;
mod blobs;
pub use blobs::*;
mod tags;
pub use tags::search_used_tags;

mod decision_history;
use decision_history::decision_history;

mod commands;
pub use commands::*;

mod routes;
pub use routes::*;

mod deadlines;
pub use deadlines::*;

mod idempotency;
pub use idempotency::*;

mod projection_consumers;
pub use projection_consumers::*;

mod tokens;
pub use tokens::*;

mod cursors;
pub use cursors::*;

#[cfg(test)]
mod tests {
    use super::{effective_statement_cache_capacity, schema_ident};

    #[test]
    fn the_statement_cache_capacity_is_explicit_then_the_urls_then_sqlxs() {
        let url = "postgres://u@h/d?sslmode=disable&statement-cache-capacity=250";
        assert_eq!(effective_statement_cache_capacity(url, Some(7)), 7);
        assert_eq!(effective_statement_cache_capacity(url, None), 250);
        assert_eq!(
            effective_statement_cache_capacity(&format!("{url}&statement-cache-capacity=3"), None),
            3
        );
        assert_eq!(
            effective_statement_cache_capacity("postgres://u@h/d?sslmode=disable", None),
            100
        );
        assert_eq!(
            effective_statement_cache_capacity("postgres://u@h/d", None),
            100
        );
    }

    #[test]
    fn schema_ident_quotes_and_escapes() {
        assert_eq!(schema_ident("banking"), "\"bc_banking\"");
        assert_eq!(
            schema_ident("x\"; DROP SCHEMA public; --"),
            "\"bc_x\"\"; DROP SCHEMA public; --\""
        );
    }

    #[test]
    fn the_capability_limit_is_one_number() {
        // `access_control::MAX_CAPABILITIES` (the pure check) and
        // `MAX_CAPABILITIES_PER_MAPPING` (the atomic SQL guard) are the one
        // `invariant CapabilitiesAreWellFormed` limit stated in two places;
        // this keeps them from drifting apart.
        assert_eq!(
            crate::access_control::MAX_CAPABILITIES,
            super::MAX_CAPABILITIES_PER_MAPPING
        );
    }
}
