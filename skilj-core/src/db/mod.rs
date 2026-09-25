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

use crate::access_control::{
    AccessLevel, CommandToken, DirectCreationToken, EventReadStartPosition, EventReadToken,
    ExternalEventToken, PrivateFieldGrant, Role, RoleAccessMapping, RoleStatus, TokenStatus,
};
use crate::bootstrap::ContextCreator;
use crate::encryption::{self, DataKey, EncryptionMasterKey};
use crate::event_store::{
    AckMode, BoundedContext, BoundedContextStatus, Command, CommandType, CursorUpdate,
    EncryptionKey, EncryptionKeyStatus, Event, EventOrigin, EventType, MissedOccurrencePolicy,
    ReadCursor,
};
use crate::projections::{Projection, ProjectionRebuild, ProjectionRebuildStatus};
use crate::shared::{Metadata, PrivateField, SensitiveField, Tag, TagMapping};
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
/// channel - called from the six DB-layer functions that change what the
/// GraphQL schema needs to expose (`upsert_event_type`/`upsert_command_type`/
/// `upsert_projection`/`insert_bounded_context`/`update_bounded_context_status`/
/// `hard_delete_bounded_context`). No payload: which exact type or bounded
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
    options.connect(database_url).await
}

/// Runs every embedded migration under `skilj-core/migrations/` - see
/// this module's own doc comment and docs/architecture.md §2.2 for why
/// `skilj-core` owns its schema this way. Only ever touches the global
/// tables - per-bounded-context schemas are provisioned separately, see
/// `provision_bounded_context_schema`.
#[tracing::instrument(skip_all)]
pub async fn migrate(pool: &Pool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}

// --- small str <-> enum conversions, shared by more than one entity below ---

fn bounded_context_status_to_str(status: BoundedContextStatus) -> &'static str {
    match status {
        BoundedContextStatus::Active => "active",
        BoundedContextStatus::Archived => "archived",
    }
}

fn bounded_context_status_from_str(s: &str) -> BoundedContextStatus {
    match s {
        "archived" => BoundedContextStatus::Archived,
        _ => BoundedContextStatus::Active,
    }
}

fn role_status_to_str(status: RoleStatus) -> &'static str {
    match status {
        RoleStatus::Active => "active",
        RoleStatus::Revoked => "revoked",
    }
}

fn role_status_from_str(s: &str) -> RoleStatus {
    match s {
        "revoked" => RoleStatus::Revoked,
        _ => RoleStatus::Active,
    }
}

fn token_status_to_str(status: TokenStatus) -> &'static str {
    match status {
        TokenStatus::Active => "active",
        TokenStatus::Revoked => "revoked",
    }
}

fn token_status_from_str(s: &str) -> TokenStatus {
    match s {
        "revoked" => TokenStatus::Revoked,
        _ => TokenStatus::Active,
    }
}

fn ack_mode_to_str(mode: AckMode) -> &'static str {
    match mode {
        AckMode::AutoAdvance => "auto_advance",
        AckMode::ManualAck => "manual_ack",
    }
}

fn ack_mode_from_str(s: &str) -> AckMode {
    match s {
        "manual_ack" => AckMode::ManualAck,
        _ => AckMode::AutoAdvance,
    }
}

fn event_read_start_position_to_str(position: EventReadStartPosition) -> &'static str {
    match position {
        EventReadStartPosition::Beginning => "beginning",
        EventReadStartPosition::Latest => "latest",
        EventReadStartPosition::AtSequence => "at_sequence",
        EventReadStartPosition::AtTime => "at_time",
    }
}

fn event_read_start_position_from_str(s: &str) -> EventReadStartPosition {
    match s {
        "latest" => EventReadStartPosition::Latest,
        "at_sequence" => EventReadStartPosition::AtSequence,
        "at_time" => EventReadStartPosition::AtTime,
        _ => EventReadStartPosition::Beginning,
    }
}

fn access_level_to_str(level: AccessLevel) -> &'static str {
    match level {
        AccessLevel::Read => "read",
        AccessLevel::Write => "write",
        AccessLevel::Admin => "admin",
    }
}

fn access_level_from_str(s: &str) -> AccessLevel {
    match s {
        "write" => AccessLevel::Write,
        "admin" => AccessLevel::Admin,
        _ => AccessLevel::Read,
    }
}

/// The Postgres schema a bounded context's own tables live in, as a
/// double-quoted identifier ready to interpolate into SQL. The `bc_`
/// prefix keeps a context from ever colliding with a real schema
/// (`public`, `pg_catalog`, ...); the quoting is defence in depth, not
/// the only thing standing between this and SQL injection - `name` is
/// only ever a value `AddBoundedContext`'s own `requires` already
/// restricted to a safe identifier pattern (specs/skilj.allium) by the
/// time it reaches here.
fn schema_ident(bounded_context: &str) -> String {
    format!("\"bc_{bounded_context}\"")
}

// --- per-bounded-context schema provisioning / hard deletion ---

/// Every `CREATE TABLE` a bounded context's own schema needs, in
/// dependency order (each table only ever references one created earlier
/// in this same list). Run once, inside the same transaction as the
/// `bounded_contexts` registry insert - see `insert_bounded_context`.
async fn provision_bounded_context_schema(
    tx: &mut Transaction<'_, Postgres>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);

    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut **tx)
        .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.event_types (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            tag_mappings JSONB NOT NULL DEFAULT '[]',
            owner_tag_key TEXT,
            sensitive_fields JSONB NOT NULL DEFAULT '[]',
            private_fields JSONB NOT NULL DEFAULT '[]',
            external_creation_allowed BOOLEAN NOT NULL,
            direct_creation_allowed BOOLEAN NOT NULL,
            system_triggered_allowed BOOLEAN NOT NULL,
            system_triggered_schedule TEXT,
            missed_occurrence_policy TEXT CHECK (missed_occurrence_policy IN \
                ('skip', 'fire_once', 'replay_backlog')),
            schedule_position TIMESTAMPTZ,
            last_fired_at TIMESTAMPTZ,
            event_read_allowed BOOLEAN NOT NULL
        )"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.command_types (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            tag_mappings JSONB NOT NULL DEFAULT '[]',
            owner_tag_key TEXT,
            sensitive_fields JSONB NOT NULL DEFAULT '[]',
            private_fields JSONB NOT NULL DEFAULT '[]',
            rest_trigger_allowed BOOLEAN NOT NULL
        )"
    )))
    .execute(&mut **tx)
    .await?;

    // See `entity EncryptionKey`. No FK to what a `SensitiveField`
    // declares it for - identified by `(subject_key, subject_value)`
    // alone, shared across every `EventType`/`CommandType` that
    // references the same subject (docs/architecture.md's write-up of
    // this pass). `wrapped_key`/`wrap_nonce` are this process's own
    // envelope-encryption detail (`skilj_core::encryption`), never part
    // of the spec's own entity - `NULL` once destroyed
    // (`db::destroy_encryption_key`), real crypto-shredding rather than
    // a status flag alone. The partial unique index is the identical
    // pattern `roles_unique_active_external_subject` already uses: at
    // most one *active* key per subject, while a destroyed one stays
    // around permanently (append-only, like everything else here) so a
    // later event for the same subject provisions a genuinely new key
    // rather than colliding with the old row.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.encryption_keys (
            id BIGSERIAL PRIMARY KEY,
            subject_key TEXT NOT NULL,
            subject_value TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('active', 'destroyed')),
            created_at TIMESTAMPTZ NOT NULL,
            destroyed_at TIMESTAMPTZ,
            wrapped_key BYTEA,
            wrap_nonce BYTEA
        )"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE UNIQUE INDEX encryption_keys_unique_active ON {schema}.encryption_keys \
         (subject_key, subject_value) WHERE status = 'active'"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.commands (
            id BIGSERIAL PRIMARY KEY,
            external_id TEXT NOT NULL UNIQUE,
            command_type_name TEXT NOT NULL REFERENCES {schema}.command_types (name),
            payload TEXT NOT NULL,
            metadata_type TEXT NOT NULL,
            metadata_version BIGINT NOT NULL,
            metadata_client_id TEXT NOT NULL,
            metadata_created_at TIMESTAMPTZ NOT NULL,
            metadata_correlation_id TEXT,
            metadata_causation_id TEXT,
            consistency_tags JSONB NOT NULL DEFAULT '[]',
            consistency_boundary BIGINT
        )"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX commands_by_created_at ON {schema}.commands (metadata_created_at)"
    )))
    .execute(&mut **tx)
    .await?;
    // Codeberg issue #18 - backs `event_store::fetch_commands`' own new
    // correlation_id filter, same "cold-start/large-history nicety, not
    // a correctness requirement" register the `events` table's own
    // sibling index below has, since that function already filters an
    // in-memory slice like every other criterion it supports.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX commands_by_correlation_id ON {schema}.commands (metadata_correlation_id) \
         WHERE metadata_correlation_id IS NOT NULL"
    )))
    .execute(&mut **tx)
    .await?;
    ensure_idempotency_keys_table(&mut **tx, bounded_context).await?;
    ensure_cross_context_route_cursors_table(&mut **tx, bounded_context).await?;
    ensure_external_message_cursors_table(&mut **tx, bounded_context).await?;
    ensure_deadline_cursors_table(&mut **tx, bounded_context).await?;
    // Codeberg issue #21 - `parked_deliveries`, a brand-new table, so no
    // `ALTER TABLE` patch is needed here the way `cross_context_route_cursors`'
    // own new retry columns need `ensure_cross_context_route_retry_columns`
    // (build()'s own startup loop only, not here - see that function's
    // own doc comment).
    ensure_parked_deliveries_table(&mut **tx, bounded_context).await?;
    // Codeberg issue #20 - `ensure_deadlines_table`'s own two-index shape
    // run directly against this transaction rather than calling that
    // function (it takes `&Pool`, not a transaction - see its own doc
    // comment), the identical split `private_field_grants_table_ddl`'s
    // own two call sites already use.
    sqlx::query(sqlx::AssertSqlSafe(deadlines_table_ddl(&schema)))
        .execute(&mut **tx)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX IF NOT EXISTS deadlines_due ON {schema}.deadlines (fire_at) \
         WHERE status = 'pending'"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX IF NOT EXISTS deadlines_tags_gin ON {schema}.deadlines USING GIN (tags)"
    )))
    .execute(&mut **tx)
    .await?;
    // `EncryptionKey` is a real, independently-lived entity (its own
    // status/lifecycle - see `entity EncryptionKey`), so it's referenced
    // here, not JSONB-embedded like `tag_mappings`/`sensitive_fields` -
    // the same distinction `projection_rebuilds`/`consumed_event_types`
    // already draw for the identical reason.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.command_encryption_keys (
            command_id BIGINT NOT NULL REFERENCES {schema}.commands (id),
            encryption_key_id BIGINT NOT NULL REFERENCES {schema}.encryption_keys (id),
            PRIMARY KEY (command_id, encryption_key_id)
        )"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.projections (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            sync BOOLEAN NOT NULL,
            caught_up_to BIGINT
        )"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.projection_consumed_event_types (
            projection_name TEXT NOT NULL REFERENCES {schema}.projections (name),
            event_type_name TEXT NOT NULL REFERENCES {schema}.event_types (name),
            PRIMARY KEY (projection_name, event_type_name)
        )"
    )))
    .execute(&mut **tx)
    .await?;

    // `PRIMARY KEY (projection_name, status)` - not `projection_name`
    // alone - is what actually lets a pending and a building row coexist
    // for the same projection (invariant `UniqueRebuildPerProjectionAndStatus`'s
    // own "at most one pending and at most one building... the two
    // coexisting is the deliberate case"), rather than structurally
    // capping every projection at one rebuild, full stop, regardless of
    // status. `projection_rebuild_consumed_event_types`/
    // `projection_rebuild_state` below both follow suit, carrying their
    // own `status` column and FK-ing against the composite key, so a
    // pending row's own consumed-types set and a building row's own
    // fold-in-progress state never get mixed up when both exist at once.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.projection_rebuilds (
            projection_name TEXT NOT NULL REFERENCES {schema}.projections (name),
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            sync BOOLEAN NOT NULL,
            caught_up_to BIGINT,
            status TEXT NOT NULL CHECK (status IN ('pending', 'building')),
            PRIMARY KEY (projection_name, status)
        )"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.projection_rebuild_consumed_event_types (
            projection_name TEXT NOT NULL,
            status TEXT NOT NULL,
            event_type_name TEXT NOT NULL REFERENCES {schema}.event_types (name),
            PRIMARY KEY (projection_name, status, event_type_name),
            FOREIGN KEY (projection_name, status)
                REFERENCES {schema}.projection_rebuilds (projection_name, status)
        )"
    )))
    .execute(&mut **tx)
    .await?;

    // A building `ProjectionRebuild`'s own private fold - "second ...
    // nothing reads until it is complete" (the note above
    // `RegisterProjection`), never the same rows as the live projection's
    // own `projection_state` below, since both can exist at once during a
    // replay window. `key` names which instance a row is (§9's "keyed /
    // multi-row Projections" pass - see the `entity Projection` note in
    // the spec) - `""` for a projection that never overrides
    // `Projection::keys()`, the same single implicit instance every such
    // projection already had. Always created lazily, on first touch, by
    // whichever of `insert_event_and_update_sync_projections`/
    // `catch_up_bounded_context` reaches this key first - there is no
    // single call site that knows every instance a projection will ever
    // have ahead of time.
    // `as_of_sequence` (Codeberg issue #25's investigation - see
    // `apply_projection_fold_update`'s own doc comment for the full
    // story): the highest event sequence actually folded into *this row*,
    // mirroring `snapshots.as_of_sequence` exactly. Without it, two
    // instances racing the same key's row lock both re-read the
    // already-folded state and fold the same event into it a second time
    // - proven by a real concurrent test, not just reasoned about.
    // `owner`, like `projection_state.owner` below, is the derived
    // owner-tag value this instance's own folded events carry - see
    // `plugin::Projection::OWNER_TAG_KEY`'s own doc comment. Nullable:
    // most projections declare no owner dimension at all.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.projection_rebuild_state (
            projection_name TEXT NOT NULL,
            status TEXT NOT NULL,
            key TEXT NOT NULL,
            state TEXT NOT NULL,
            owner TEXT,
            as_of_sequence BIGINT NOT NULL DEFAULT -1,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (projection_name, status, key),
            FOREIGN KEY (projection_name, status)
                REFERENCES {schema}.projection_rebuilds (projection_name, status)
        )"
    )))
    .execute(&mut **tx)
    .await?;

    // A sync projection's own materialised state (`project()`'s own
    // fold output, JSON-encoded, one row per instance/`key` - see the
    // note on `projection_rebuild_state` just above) - see
    // docs/architecture.md's write-up of §8 item 6 and §9's "keyed /
    // multi-row Projections" pass. Created lazily, on first touch, not
    // seeded at registration time - a projection's own instances aren't
    // known until events actually name them.
    //
    // `owner` (cross-tenant projection read fix, docs/architecture.md's
    // own write-up of this pass): this instance's own derived owner-tag
    // value, or null when the projection declares no
    // `Projection::OWNER_TAG_KEY` or no consuming event has supplied one
    // yet. Set/refreshed by whichever fold call site (`insert_event_and_
    // update_sync_projections_in_tx`/`catch_up_bounded_context`/
    // `fold_history_into_new_sync_projection`) touches this row -
    // `apply_projection_fold_update`'s own doc comment. Read by
    // `get_projection_state`/`projections::query_projection`'s own
    // enforcement.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.projection_state (
            projection_name TEXT NOT NULL REFERENCES {schema}.projections (name),
            key TEXT NOT NULL,
            state TEXT NOT NULL,
            owner TEXT,
            as_of_sequence BIGINT NOT NULL DEFAULT -1,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (projection_name, key)
        )"
    )))
    .execute(&mut **tx)
    .await?;
    // Codeberg issue #25 - `projection_partition_progress`, a brand-new
    // table, so `ensure_projection_partition_progress_table`'s own
    // `CREATE TABLE IF NOT EXISTS` is the whole story here too - see its
    // own doc comment. Must run after `projections` (just above) exists,
    // since its own `projection_name` column references it.
    ensure_projection_partition_progress_table(&mut **tx, bounded_context).await?;

    // Backs `next_sequence` - one row, seeded below, incremented under
    // the row lock its own `UPDATE ... RETURNING` acquires. `-1` is
    // "nothing allocated yet", so the first call returns `0` - the same
    // convention `after_sequence.unwrap_or(-1)`/`highest_sequence`'s
    // `None` case use throughout skilj-core.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.sequence (next_value BIGINT NOT NULL)"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.sequence (next_value) VALUES (-1)"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.events (
            sequence BIGINT PRIMARY KEY,
            event_type_name TEXT NOT NULL REFERENCES {schema}.event_types (name),
            payload TEXT NOT NULL,
            metadata_type TEXT NOT NULL,
            metadata_version BIGINT NOT NULL,
            metadata_client_id TEXT NOT NULL,
            metadata_created_at TIMESTAMPTZ NOT NULL,
            metadata_correlation_id TEXT,
            metadata_causation_id TEXT,
            tags JSONB NOT NULL DEFAULT '[]',
            origin_kind TEXT NOT NULL CHECK (
                origin_kind IN ('external_triggered', 'directly_created', 'command_triggered', 'system_triggered')
            ),
            origin_source_content TEXT,
            origin_source_context TEXT,
            origin_command_id BIGINT REFERENCES {schema}.commands (id)
        )"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX events_by_type ON {schema}.events (event_type_name, sequence)"
    )))
    .execute(&mut **tx)
    .await?;
    // Codeberg issue #18 - backs `event_store::query_events`/`count_events`'
    // own new correlation_id filter. Same "cold-start/large-history
    // nicety, not a correctness requirement" register as `events_by_tags`
    // below: both functions already operate on an in-memory slice.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX events_by_correlation_id ON {schema}.events (metadata_correlation_id) \
         WHERE metadata_correlation_id IS NOT NULL"
    )))
    .execute(&mut **tx)
    .await?;
    // docs/architecture.md §19's "Problem 1" fix -
    // `list_events_for_bounded_context_matching_tags`' own GIN index.
    // `tags` is already `JSONB`, so this needs no column-type change:
    // GIN indexes each array element's key/value pairs, which is what
    // makes a `tags @> '[{"key":"...","value":"..."}]'::jsonb`
    // containment query fast rather than a sequential scan. Only
    // bounded contexts provisioned after this change get it - a
    // pre-existing one would need `CREATE INDEX ... USING GIN (tags)`
    // run by hand, see that function's own doc comment for why no
    // backfill migration exists for this yet.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX events_by_tags ON {schema}.events USING GIN (tags)"
    )))
    .execute(&mut **tx)
    .await?;
    // docs/architecture.md §19's "Problem 2" fix - `Snapshot`'s own
    // storage, deliberately not shared with `projection_state` above
    // (see `crate::plugin::Snapshot`'s own doc comment for why). One
    // row per `(snapshot_name, tag_key, tag_value)` - `tag_value` alone
    // isn't unique across different snapshot definitions that happen to
    // share a `tag_key`, hence the three-column key rather than two.
    // `snapshot_version` is checked against `Snapshot::VERSION` before a
    // row is ever trusted - a mismatch is treated as if the row doesn't
    // exist, never read, so there's no `CHECK`/foreign key tying it to
    // anything: an old-version row is inert data until the next catch-up
    // tick overwrites it.
    // `owner` (cross-tenant read fix, docs/architecture.md's own
    // write-up of these passes): this row's own derived owner-tag value,
    // or null when the snapshot declares no `Snapshot::OWNER_TAG_KEY` or
    // no folded event has supplied one yet - `catch_up_snapshots`' own
    // fold loop sets/refreshes it. Read by `get_snapshot_state_and_owner`/
    // `snapshot_query::inspect_snapshot_field`'s own enforcement.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.snapshots (
            snapshot_name TEXT NOT NULL,
            tag_key TEXT NOT NULL,
            tag_value TEXT NOT NULL,
            snapshot_version BIGINT NOT NULL,
            as_of_sequence BIGINT NOT NULL,
            state JSONB NOT NULL,
            owner TEXT,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (snapshot_name, tag_key, tag_value)
        )"
    )))
    .execute(&mut **tx)
    .await?;
    // The background catch-up task's own position, one row per
    // `snapshot_name` - `Projection`'s `caught_up_to` column plays the
    // identical role, but isn't reused here (see `crate::plugin::Snapshot`'s
    // own doc comment): `MAX(as_of_sequence)` across `snapshots` rows
    // above can't stand in for this, since a rarely-touched tag value's
    // own row can be correctly stale (nothing has happened for it)
    // without the walk itself being behind.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.snapshot_progress (
            snapshot_name TEXT PRIMARY KEY,
            caught_up_to BIGINT NOT NULL
        )"
    )))
    .execute(&mut **tx)
    .await?;
    // Codeberg issue #25 (docs/architecture.md §52) -
    // `snapshot_partition_progress`, a brand-new table, so
    // `ensure_snapshot_partition_progress_table`'s own `CREATE TABLE IF
    // NOT EXISTS` is the whole story here too - see its own doc
    // comment. No ordering constraint to worry about (unlike
    // `projection_partition_progress`'s own `REFERENCES {schema}.projections`):
    // `snapshots`/`snapshot_progress` have no backing registration table
    // either, so this one doesn't reference anything.
    ensure_snapshot_partition_progress_table(&mut **tx, bounded_context).await?;
    // See `command_encryption_keys` above - the identical join-table
    // treatment, keyed by `sequence` instead of a synthetic id since
    // `events.sequence` is already `Event`'s own natural primary key.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.event_encryption_keys (
            event_sequence BIGINT NOT NULL REFERENCES {schema}.events (sequence),
            encryption_key_id BIGINT NOT NULL REFERENCES {schema}.encryption_keys (id),
            PRIMARY KEY (event_sequence, encryption_key_id)
        )"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.access_tokens (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK (kind IN ('external_event', 'direct_creation', 'event_read', 'command')),
            secret TEXT NOT NULL, -- hash_secret's output, never the plaintext (see AccessToken.secret)
            status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
            created_at TIMESTAMPTZ NOT NULL,
            revoked_at TIMESTAMPTZ,
            event_type_name TEXT REFERENCES {schema}.event_types (name),
            command_type_name TEXT REFERENCES {schema}.command_types (name),
            -- EventReadToken.scope/ExternalEventToken.scope/
            -- DirectCreationToken.scope/CommandToken.scope - one column
            -- for all four kinds, the cross-tenant read/write fix
            -- (docs/architecture.md's own write-up of these passes).
            -- Null (the default before these fixes existed, and still
            -- the default for a token minted with none) means
            -- unrestricted, for every kind alike.
            scope TEXT,
            -- EventReadToken.start_from/.start_at_sequence/.start_at_time -
            -- meaningful only for the 'event_read' kind (see
            -- ensure_event_read_token_start_from_column's own doc comment
            -- for why a fresh table still declares these with the
            -- identical defaults a retrofitted one gets).
            start_from TEXT NOT NULL DEFAULT 'beginning'
                CHECK (start_from IN ('beginning', 'latest', 'at_sequence', 'at_time')),
            start_at_sequence BIGINT,
            start_at_time TIMESTAMPTZ,
            CHECK (
                (kind = 'command' AND command_type_name IS NOT NULL AND event_type_name IS NULL)
                OR (kind != 'command' AND event_type_name IS NOT NULL AND command_type_name IS NULL)
            )
        )"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.read_cursors (
            token_id TEXT PRIMARY KEY REFERENCES {schema}.access_tokens (id),
            ack_mode TEXT NOT NULL CHECK (ack_mode IN ('auto_advance', 'manual_ack')),
            sequence BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            checked_out_at TIMESTAMPTZ
        )"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(private_field_grants_table_ddl(&schema)))
        .execute(&mut **tx)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX private_field_grants_by_grantee ON {schema}.private_field_grants (grantee_role_id, status)"
    )))
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `entity PrivateFieldGrant` - `event_sequence`/`command_id` are this
/// table's own foreign keys into `events`/`commands`, one column each
/// rather than the `AccessToken`-style single discriminated pair, since
/// both may be null at once (a blanket grant) but never both non-null -
/// enforced here, not just by `access_control::grant_private_field_access_for_event`/
/// `_for_command` alone, the same "the invariant is a real constraint,
/// not merely an implication of how callers happen to behave" stance
/// `access_tokens`' own `kind`-vs-`event_type_name`/`command_type_name`
/// CHECK already takes. `command_id` references `commands.id` (the
/// internal `BIGSERIAL`, not `Command.id`/`commands.external_id`) -
/// `origin_command_id` on `events` already sets this precedent.
/// `grantor_role_id`/`grantee_role_id` reference `public.roles (id)`
/// across schemas - Postgres allows this freely, and `roles` is
/// guaranteed to exist by the time any bounded context is provisioned
/// (the global migration set runs first, at `db::migrate()`). Shared
/// between `provision_bounded_context_schema` (a brand-new context) and
/// `ensure_private_field_grants_table` below (an existing one, added
/// after this table existed) - one DDL string, not two copies to keep in
/// sync.
fn private_field_grants_table_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {schema}.private_field_grants (
            id TEXT PRIMARY KEY,
            grantor_role_id TEXT NOT NULL REFERENCES public.roles (id),
            grantee_role_id TEXT NOT NULL REFERENCES public.roles (id),
            event_sequence BIGINT REFERENCES {schema}.events (sequence),
            command_id BIGINT REFERENCES {schema}.commands (id),
            status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
            created_at TIMESTAMPTZ NOT NULL,
            revoked_at TIMESTAMPTZ,
            CHECK (event_sequence IS NULL OR command_id IS NULL)
        )"
    )
}

/// `ensure_idempotency_keys_table`'s own doc comment's "no general
/// per-bounded-context schema migration mechanism" applies identically
/// here, just for `private_field_grants` instead of `idempotency_keys` -
/// called unconditionally on every `build()`, for a bounded context
/// provisioned before this table existed.
pub async fn ensure_private_field_grants_table(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(private_field_grants_table_ddl(&schema)))
        .execute(pool)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX IF NOT EXISTS private_field_grants_by_grantee ON {schema}.private_field_grants (grantee_role_id, status)"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

// --- PrivateFieldGrant ---

/// `commands.id` (the internal `BIGSERIAL`) for a given `Command.id`
/// (`commands.external_id`) - the forward half of the translation
/// `insert_private_field_grant` needs; `get_command_by_id` already is the
/// reverse (see its own doc comment, and the note on `PrivateFieldGrant.
/// command_id` in `access_control` for why a grant stores the internal
/// id rather than the external one).
async fn command_internal_id(
    pool: &Pool,
    bounded_context: &str,
    external_id: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {schema}.commands WHERE external_id = $1"
    )))
    .bind(external_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id,)| id))
}

#[derive(sqlx::FromRow)]
struct PrivateFieldGrantRow {
    id: String,
    grantor_role_id: String,
    grantee_role_id: String,
    event_sequence: Option<i64>,
    command_id: Option<i64>,
    status: String,
    created_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
}

impl PrivateFieldGrantRow {
    /// `Ok(None)` - not a panic - when `grantor_role_id`/`grantee_role_id`
    /// names a `Role` that's gone by the time this looks it up, the same
    /// "gone by the time you look" treatment `RoleAccessMappingRow::
    /// into_domain` already gives an analogous race (see that method's
    /// own doc comment, docs/architecture.md's own write-up of that
    /// hardening pass) - `Role` rows are never hard-deleted in this
    /// codebase, so this is a defensive match for a race that shouldn't
    /// occur in practice, not one known to.
    async fn into_domain(
        self,
        pool: &Pool,
        bounded_context: &BoundedContext,
    ) -> crate::error::Result<Option<PrivateFieldGrant>> {
        let Some(grantor) = get_role(pool, &self.grantor_role_id).await? else {
            return Ok(None);
        };
        let Some(grantee) = get_role(pool, &self.grantee_role_id).await? else {
            return Ok(None);
        };
        let command_id = match self.command_id {
            Some(internal_id) => {
                match get_command_by_id(pool, &bounded_context.name, internal_id).await? {
                    Some(command) => Some(command.id),
                    None => return Ok(None),
                }
            }
            None => None,
        };
        Ok(Some(PrivateFieldGrant {
            id: self.id,
            bounded_context: bounded_context.clone(),
            grantor,
            grantee,
            event_sequence: self.event_sequence,
            command_id,
            status: token_status_from_str(&self.status),
            created_at: self.created_at,
            revoked_at: self.revoked_at,
        }))
    }
}

const PRIVATE_FIELD_GRANT_COLUMNS: &str =
    "id, grantor_role_id, grantee_role_id, event_sequence, command_id, status, created_at, revoked_at";

#[tracing::instrument(skip_all)]
pub async fn insert_private_field_grant(
    pool: &Pool,
    grant: &PrivateFieldGrant,
) -> crate::error::Result<()> {
    let schema = schema_ident(&grant.bounded_context.name);
    let command_id = match grant.command_id.as_deref() {
        Some(external_id) => Some(
            command_internal_id(pool, &grant.bounded_context.name, external_id)
                .await?
                .expect(
                    "insert_private_field_grant: command_id names a command that doesn't exist",
                ),
        ),
        None => None,
    };
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.private_field_grants ({PRIVATE_FIELD_GRANT_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
    )))
    .bind(&grant.id)
    .bind(&grant.grantor.id)
    .bind(&grant.grantee.id)
    .bind(grant.event_sequence)
    .bind(command_id)
    .bind(token_status_to_str(grant.status))
    .bind(grant.created_at)
    .bind(grant.revoked_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Every `PrivateFieldGrant` in one bounded context, active or revoked
/// alike - `ListPrivateFieldGrants` itself reads both back (see its own
/// `@guidance`: "what have I shared, and what have I stopped sharing").
/// Also what `render_event`/`render_command`'s own callers pre-load and
/// pass through as `grants` - one shared snapshot, filtered internally
/// by `grantee` (see those functions' own doc comments for why that's
/// enough for every caller in one request, subscribers included).
#[tracing::instrument(skip_all)]
pub async fn list_private_field_grants_for_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<PrivateFieldGrant>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<PrivateFieldGrantRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PRIVATE_FIELD_GRANT_COLUMNS} FROM {schema}.private_field_grants"
    )))
    .fetch_all(pool)
    .await?;
    let mut grants = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(grant) = row.into_domain(pool, &bc).await? {
            grants.push(grant);
        }
    }
    Ok(grants)
}

#[tracing::instrument(skip_all)]
pub async fn get_private_field_grant(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<PrivateFieldGrant>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let row: Option<PrivateFieldGrantRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PRIVATE_FIELD_GRANT_COLUMNS} FROM {schema}.private_field_grants WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(row) => row.into_domain(pool, &bc).await,
        None => Ok(None),
    }
}

/// Persists a `revoke_private_field_access` outcome - a status-only
/// update, the same shape `revoke_active_role_access_mapping` already
/// has, addressed by `id` rather than a composite key since a grant's
/// own `id` is already unambiguous (see `PrivateFieldGrant.id`'s own doc
/// comment).
#[tracing::instrument(skip_all)]
pub async fn update_private_field_grant(
    pool: &Pool,
    grant: &PrivateFieldGrant,
) -> crate::error::Result<()> {
    let schema = schema_ident(&grant.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.private_field_grants SET status = $1, revoked_at = $2 WHERE id = $3"
    )))
    .bind(token_status_to_str(grant.status))
    .bind(grant.revoked_at)
    .bind(&grant.id)
    .execute(pool)
    .await?;
    Ok(())
}

/// `idempotency_keys` - a caller-supplied idempotency key on command
/// submission (Codeberg issue #12), one row per `(command_type_name,
/// client_id, idempotency_key)` that has ever produced a real `Accepted`
/// outcome. A duplicate submission bearing the same key from the same
/// caller short-circuits to the stored `triggered_event_sequences`
/// rather than being re-decided - see `submit_command`'s own doc
/// comment for the full design.
///
/// `client_id`-scoped since [docs/architecture.md §37](../../../docs/architecture.md#idempotency-keys-client-id-scoping) - originally
/// `(command_type_name, idempotency_key)` only (issue #12's own
/// deliberate call: "not also per-caller"), which held up fine under
/// that decision's own assumption - a well-randomized caller-chosen key
/// (a UUID, say) never collides with another caller's by accident. Owner-
/// tag multi-tenancy ([§23](../../../docs/architecture.md#cross-tenant-projection-read-fix-owner-tag)/[§25](../../../docs/architecture.md#cross-tenant-read-fix-command-query)/[§30](../../../docs/architecture.md#cross-tenant-write-fix-owner-tag-scoping)), built after issue #12, broke that
/// assumption: many distinct tenants (distinct `CommandToken`s/
/// `RoleAccessMapping`s, disambiguated only by their own `scope`,
/// invisible to this table) routinely submit the *same* `CommandType`,
/// and a business-derived key (an order id, an invoice number - a
/// common, even recommended, idempotency-key convention) from one
/// tenant can plausibly coincide with an unrelated tenant's own, with no
/// attacker needed at all. A collision silently swallowed the second
/// tenant's real submission as a `Deduplicated` hit against the first
/// tenant's own stored sequences - a live bug since issue #12 shipped in
/// 0.0.2, not merely `CrossContextRoute`'s narrower predictable-key
/// variant ([§36](../../../docs/architecture.md#cross-context-route)) of the same root cause. `client_id` (`token.id` for
/// REST, `access_mapping.role.id` for GraphQL, `"cross-context-route"`
/// for that internal caller - `authorise_command_trigger`/
/// `authorise_command_submission`'s own `CommandAuthorised.client_id`)
/// was already threaded through every caller for the resulting event's
/// own metadata; it's simply never scoped the idempotency lookup before
/// now. This structurally closes `CrossContextRoute`'s own issue too,
/// since no external caller's `client_id` is ever caller-suppliable -
/// it's always derived server-side from an authenticated token/role, so
/// no external submission can ever land under `"cross-context-route"`'s
/// own partition regardless of what `idempotency_key` string it uses.
/// `RESERVED_IDEMPOTENCY_KEY_PREFIX`/`reject_reserved_idempotency_key`
/// stay in place as harmless defense-in-depth, no longer load-bearing.
///
/// See `migrate_idempotency_keys_client_id_scoping` for how an
/// already-provisioned bounded context (real ones exist, back to 0.0.2)
/// gets patched onto this shape - this function alone only ever governs
/// a brand-new one.
///
/// `impl PgExecutor`, the same "works on `&Pool` autocommit or inside a
/// caller's own open `Transaction`" treatment `update_role` already
/// gets: `provision_bounded_context_schema` above needs the latter (one
/// more table for a brand-new bounded context, inside its own
/// provisioning transaction); the per-bounded-context startup loop in
/// `skilj/src/lib.rs` needs the former, patching a bounded context
/// provisioned *before* this feature existed. `CREATE TABLE IF NOT
/// EXISTS`, called unconditionally on every `build()`, is the whole
/// migration story for a table that doesn't exist yet at all - there's
/// no general per-bounded-context schema migration mechanism in this
/// codebase (`provision_bounded_context_schema` itself only ever runs
/// once, at creation), and this deliberately isn't one either, just a
/// small, targeted, idempotent patch for this one table's existence.
#[tracing::instrument(skip_all)]
pub async fn ensure_idempotency_keys_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.idempotency_keys (
            command_type_name TEXT NOT NULL,
            client_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            triggered_event_sequences BIGINT[] NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (command_type_name, client_id, idempotency_key)
        )"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// Patches an already-provisioned bounded context's `idempotency_keys`
/// (real ones exist, back to 0.0.2 - see `ensure_idempotency_keys_table`'s
/// own doc comment for the full story) onto the `client_id`-scoped shape
/// a brand-new one gets directly. `ALTER TABLE ... ADD COLUMN IF NOT
/// EXISTS`, this file's own established idempotent-patch idiom
/// (`ensure_projection_state_owner_columns`/`ensure_event_scoping_columns`),
/// isn't enough by itself here - the whole point is this column must be
/// *part of the primary key*, and Postgres has no `ADD CONSTRAINT IF NOT
/// EXISTS`/`ALTER PRIMARY KEY` form to lean on for that half.
///
/// Backfilled `''` (never a real `client_id` - always a server-derived
/// token/role id or `"cross-context-route"`, never empty) rather than
/// deleted, matching this codebase's own explicit "no retention/TTL,
/// nothing is ever deleted" precedent ([§21](../../../docs/architecture.md#optional-idempotency-key-submission)) - but, on the user's own
/// explicit call, deliberately left permanently *unmatchable* by
/// `lookup_idempotency_key` rather than kept as a fallback for whichever
/// caller retries that same string first. Two ways to fail were on the
/// table for a row this migration cannot attribute to its original
/// caller (that information was simply never recorded pre-migration,
/// not recoverable by any cleverer migration): keep it matchable for
/// anyone, which fully closes future double-execution risk but leaves a
/// frozen, non-growing set of already-used key strings still able to
/// collide across unrelated future callers; or retire it, which fully
/// closes *that* risk but means a genuine retry of a request submitted
/// just before this migration ran won't be recognised as a duplicate -
/// `decide()` runs again, possibly inserting a real command's events
/// twice. Chosen: retire it - this fix exists specifically to close the
/// cross-tenant collision class, and leaving any part of it open, even
/// a shrinking one, was judged worse than the narrower, one-time
/// migration-boundary risk. The row stays in the table (never deleted),
/// permanently orphaned rather than reachable.
///
/// Wrapped in one transaction (fine - Postgres DDL is fully
/// transactional, unlike MySQL's) holding a `pg_advisory_xact_lock`
/// keyed by this bounded context's own schema name for its entire
/// duration - not because skipping it would be unsafe (verified
/// directly, not assumed: with the lock removed, several concurrent
/// callers racing the exact same pre-migration table in a real test
/// never errored, because `DROP CONSTRAINT IF EXISTS` before `ADD
/// PRIMARY KEY` means there is never an *existing* primary key for a
/// second `ADD PRIMARY KEY` to collide with, and Postgres's own
/// whole-transaction-duration `ACCESS EXCLUSIVE` table lock from the
/// first `ALTER TABLE` already fully serializes every concurrent
/// instance's 3-statement sequence against this same table). The real,
/// more modest reason to keep it: without it, every *loser* of that
/// natural serialization still repeats the whole `DROP CONSTRAINT`/`ADD
/// PRIMARY KEY` dance once it's their turn (each is a genuine
/// idempotent no-op end state, but a real catalog write nonetheless,
/// once per racer) - the lock lets every loser's own upfront
/// `already_migrated` recheck below see the winner's now-committed
/// change and skip straight to a plain read, so only the first instance
/// through ever does real DDL work, and two skilj instances patching
/// the same shared Postgres at startup (the existing
/// concurrent-bounded-context warm-up loop in `skilj/src/lib.rs`,
/// Codeberg issue #15, only protects against a race *within* one
/// process - a real fleet runs more than one) don't churn the catalog
/// once each for no reason. `_xact` (transaction-scoped, not
/// session-scoped) releases automatically at this function's own commit
/// or rollback and is guaranteed to run on the same connection as the
/// statements it protects, both being inside the one transaction -
/// unlike a bare `pg_advisory_lock` against a `&Pool`, where the lock
/// and the work it protects could each be handed a different pooled
/// connection entirely, making the lock meaningless.
#[tracing::instrument(skip_all)]
pub async fn migrate_idempotency_keys_client_id_scoping(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    // Mirrors `schema_ident`'s own `"bc_{bounded_context}"` shape, minus
    // the quoting - `information_schema` stores identifiers unquoted
    // (quoting is parse-time syntax, not a stored property), so a query
    // against it needs the raw name, not the `format!`-ready quoted one
    // every DDL string above uses.
    let raw_schema = format!("bc_{bounded_context}");

    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(&raw_schema)
        .execute(&mut *tx)
        .await?;

    // Checked against the primary key specifically, not merely the
    // column's existence - robust even against a hypothetical partially-
    // applied prior attempt (column added, PK swap not yet reached).
    let already_migrated: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.key_column_usage
            WHERE table_schema = $1 AND table_name = 'idempotency_keys'
              AND constraint_name = 'idempotency_keys_pkey'
              AND column_name = 'client_id'
        )",
    )
    .bind(&raw_schema)
    .fetch_one(&mut *tx)
    .await?;

    if already_migrated {
        tx.commit().await?;
        return Ok(());
    }

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.idempotency_keys \
         ADD COLUMN IF NOT EXISTS client_id TEXT NOT NULL DEFAULT ''"
    )))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.idempotency_keys DROP CONSTRAINT IF EXISTS idempotency_keys_pkey"
    )))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.idempotency_keys \
         ADD PRIMARY KEY (command_type_name, client_id, idempotency_key)"
    )))
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// `cross_context_route_cursors` - one row per registered
/// `CrossContextRoute`, in its own `Source`'s bounded-context schema
/// (the same "cursor lives with whoever's reading" register `ReadCursor`
/// already establishes, just for an internal reader rather than an
/// external `EventReadToken` holder). `last_dispatched_sequence` uses
/// the same `-1` "nothing yet" sentinel `sequence`/`caught_up_to`
/// already use throughout this codebase, rather than a nullable column.
/// `ensure_idempotency_keys_table`'s own doc comment's "no general
/// per-bounded-context schema migration mechanism" applies identically
/// here - `CREATE TABLE IF NOT EXISTS`, patched into every bounded
/// context on every `build()`, is the whole migration story.
///
/// `retry_attempt_count`/`retry_first_failed_at`/`retry_next_attempt_at`
/// (Codeberg issue #21) - the durable backoff state one route's own
/// blocked head-of-line occurrence carries between ticks, applying
/// `skilj_retry::RetryPolicy` to what used to be an unbounded, un-
/// throttled "retry every single tick forever" - see
/// `catch_up_cross_context_route`'s own doc comment for the full
/// mechanism. A single row's worth of state suffices (rather than, say,
/// a row per failing occurrence): only the cursor's own next occurrence
/// can ever be the one currently failing, since the cursor never
/// advances past it - the same invariant that already lets
/// `last_dispatched_sequence` be a single column rather than a set. An
/// already-provisioned bounded context gets these columns patched in
/// separately by `ensure_cross_context_route_retry_columns` (this
/// `CREATE TABLE IF NOT EXISTS` is a no-op against an existing table, so
/// it can't add columns to one - see that function's own doc comment).
#[tracing::instrument(skip_all)]
pub async fn ensure_cross_context_route_cursors_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.cross_context_route_cursors (
            route_name TEXT PRIMARY KEY,
            last_dispatched_sequence BIGINT NOT NULL DEFAULT -1,
            updated_at TIMESTAMPTZ NOT NULL,
            retry_attempt_count INT NOT NULL DEFAULT 0,
            retry_first_failed_at TIMESTAMPTZ,
            retry_next_attempt_at TIMESTAMPTZ
        )"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// Patches an already-provisioned bounded context's
/// `cross_context_route_cursors` (a real table since the cross-context
/// router itself shipped) onto the retry-columns shape a brand-new one
/// gets directly from `ensure_cross_context_route_cursors_table` above.
/// `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, the same idempotent-patch
/// idiom `ensure_projection_state_owner_columns`/`ensure_event_scoping_columns`
/// already use - simpler than that pair's own PK-migration sibling
/// (`migrate_idempotency_keys_client_id_scoping`) needs, since none of
/// these three columns join a primary key. Called unconditionally on
/// every `build()`, alongside `ensure_cross_context_route_cursors_table`
/// itself - a no-op once a bounded context already has them, migrated or
/// fresh.
pub async fn ensure_cross_context_route_retry_columns(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.cross_context_route_cursors \
         ADD COLUMN IF NOT EXISTS retry_attempt_count INT NOT NULL DEFAULT 0"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.cross_context_route_cursors \
         ADD COLUMN IF NOT EXISTS retry_first_failed_at TIMESTAMPTZ"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.cross_context_route_cursors \
         ADD COLUMN IF NOT EXISTS retry_next_attempt_at TIMESTAMPTZ"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Codeberg issue #20 - the durable cursor table `catch_up_schedule_deadline`/
/// `catch_up_cancel_deadline` share, one row per registered
/// `ScheduleDeadline::NAME`/`CancelDeadline::NAME` (a schedule and its own
/// cancel counterpart advance independently, even though they're paired -
/// see `plugin::CancelDeadline::Deadline`'s own doc comment). Deliberately
/// its own table rather than piggybacked on `cross_context_route_cursors`
/// above - a genuinely different reactor family, even though the cursor
/// shape (`{owner} -> last_dispatched_sequence`) is identical. Same
/// `impl PgExecutor`/`CREATE TABLE IF NOT EXISTS` treatment as every
/// sibling `ensure_*_table` function here - see
/// `ensure_idempotency_keys_table`'s own doc comment for the full
/// "brand-new bounded context vs. patching an already-provisioned one"
/// story.
pub async fn ensure_deadline_cursors_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.deadline_cursors (
            cursor_owner TEXT PRIMARY KEY,
            last_dispatched_sequence BIGINT NOT NULL DEFAULT -1,
            updated_at TIMESTAMPTZ NOT NULL
        )"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// Codeberg issue #25's investigation (docs/architecture.md §50/§51) -
/// one row per `(projection_name, partition_index)` a partitioned async
/// `Projection` (`Projection::PARTITION_COUNT > 1`) has ever had a
/// `catch_up_partitioned_projection` tick seed or advance. `caught_up_to`
/// here is this one partition's own position, distinct from
/// `projections.caught_up_to` - the latter stays the single external
/// source of truth (`Projection.caughtUpTo` over GraphQL,
/// `wait_until_caught_up`), rolled up as the `MIN(caught_up_to)` across
/// a projection's own partition rows every tick. `DEFAULT -1` matches
/// `projection_state.as_of_sequence`'s own "nothing folded yet"
/// convention. Same `impl PgExecutor`/`CREATE TABLE IF NOT EXISTS`
/// treatment as every sibling `ensure_*_table` function here - see
/// `ensure_idempotency_keys_table`'s own doc comment for the full
/// "brand-new bounded context vs. patching an already-provisioned one"
/// story.
pub async fn ensure_projection_partition_progress_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.projection_partition_progress (
            projection_name TEXT NOT NULL REFERENCES {schema}.projections (name),
            partition_index INT NOT NULL,
            caught_up_to BIGINT NOT NULL DEFAULT -1,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (projection_name, partition_index)
        )"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// `Snapshot`'s own twin of `ensure_projection_partition_progress_table`
/// just above - one row per `(snapshot_name, partition_index)` a
/// partitioned `Snapshot` (`Snapshot::PARTITION_COUNT > 1`) has ever had
/// a `catch_up_partitioned_snapshot` tick seed or advance
/// (docs/architecture.md §52). `caught_up_to` is this one partition's
/// own position, distinct from `snapshot_progress.caught_up_to` - the
/// latter stays the single external rollup (`MIN(caught_up_to)` across
/// a snapshot's own partition rows every tick), the identical relation
/// `projections.caught_up_to`/`projection_partition_progress` already
/// has. `DEFAULT -1` matches `snapshots.as_of_sequence`'s own "nothing
/// folded yet" convention. No `REFERENCES` clause, unlike the
/// `Projection` twin - `snapshots` has no backing registration table to
/// reference, so there's nothing to foreign-key against and no
/// table-creation-ordering constraint here.
pub async fn ensure_snapshot_partition_progress_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.snapshot_partition_progress (
            snapshot_name TEXT NOT NULL,
            partition_index INT NOT NULL,
            caught_up_to BIGINT NOT NULL DEFAULT -1,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (snapshot_name, partition_index)
        )"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// Codeberg issue #20's own row store - one row per deadline a
/// `ScheduleDeadline` has ever scheduled, living in
/// `ScheduleDeadline::Source::BOUNDED_CONTEXT`'s own schema (mirroring
/// `cross_context_route_cursors`' placement choice: the reactor's own
/// *source* side owns the durable state, regardless of where its
/// *target* eventually lands). `id` is deterministic
/// (`"{schedule_name}:{source_event_sequence}"`, see `ScheduleDeadline::NAME`'s
/// own doc comment) so a redelivered catch-up tick's `INSERT ... ON
/// CONFLICT (id) DO NOTHING` is always a safe no-op, never a duplicate
/// row. `tags` backs `catch_up_cancel_deadline`'s own tag-containment
/// lookup, indexed the same GIN way `docs/architecture.md §19 Problem 1`
/// already indexes the `events` table's own `tags` column. `status`
/// starts `'pending'`, and moves to `'cancelled'` (`catch_up_cancel_deadline`,
/// terminal) or `'firing'` then `'fired'` (`db::fire_due_deadlines`'s own
/// atomic claim-then-resolve, closing the fire-vs-cancel race - see
/// [docs/architecture.md §55](../../../docs/architecture.md#55-closing-the-canceldeadline-fire-vs-cancel-race)).
/// `firing_at` records when a row was claimed, purely so a crashed
/// instance's claim can be reclaimed after it goes stale - see
/// `fire_due_deadlines`'s own doc comment for the reclaim window.
///
/// The DDL itself is shared between `provision_bounded_context_schema`
/// (a brand-new context, run against its own open transaction) and
/// `ensure_deadlines_table` below (an existing one, run against `&Pool`) -
/// one DDL string, not two copies to keep in sync, the identical split
/// `private_field_grants_table_ddl` already uses for the same reason.
fn deadlines_table_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {schema}.deadlines (
            id TEXT PRIMARY KEY,
            schedule_name TEXT NOT NULL,
            fire_at TIMESTAMPTZ NOT NULL,
            tags JSONB NOT NULL,
            correlation_id TEXT,
            target_bounded_context TEXT NOT NULL,
            target_command_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            resolved_at TIMESTAMPTZ,
            firing_at TIMESTAMPTZ
        )"
    )
}

/// `pool: &Pool`, not a generic `impl PgExecutor` - this needs three
/// statements (the table plus two indexes), and unlike every
/// single-statement sibling `ensure_*_table` function here, a generic
/// executor can't be reused across more than one `.execute()` call
/// without already being `Copy` the way `&Pool` is. Same split
/// `ensure_private_field_grants_table` already uses for the identical
/// reason: `provision_bounded_context_schema` below runs the equivalent
/// statements directly against its own `&mut **tx` instead of calling
/// this.
pub async fn ensure_deadlines_table(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(deadlines_table_ddl(&schema)))
        .execute(pool)
        .await?;
    // A bounded context provisioned before the fire-vs-cancel race fix
    // (docs/architecture.md §55) gets `firing_at` patched in here -
    // nullable, no `DEFAULT`, same treatment `ensure_read_cursors_checkout_column`
    // already gives `checked_out_at` for the identical reason: `NULL`
    // already means exactly "not currently claimed."
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.deadlines ADD COLUMN IF NOT EXISTS firing_at TIMESTAMPTZ"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX IF NOT EXISTS deadlines_due ON {schema}.deadlines (fire_at) \
         WHERE status = 'pending'"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX IF NOT EXISTS deadlines_tags_gin ON {schema}.deadlines USING GIN (tags)"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `external_message_cursors` - the durable state behind
/// `highest_dedupe_sequence(adapter, dedupe_partition_key)` in
/// specs/skilj.allium's own `rule CreateExternalEvent`
/// ([docs/architecture.md §39](../../../docs/architecture.md#external-message-dedup-create-external-event)). One row per `(adapter_id, partition_key)`
/// pair that has ever had an event written under it, storing only the
/// highest `dedupe_sequence` seen so far - a compact watermark, not a row
/// per message the way `idempotency_keys` is: sound only because the
/// caller's own external source (Kafka, Kinesis, Pulsar and their kin)
/// already guarantees strictly increasing delivery order within one
/// partition, which is exactly the property that makes remembering
/// anything below the highest seen unnecessary. See
/// `db::create_and_insert_external_event`'s own doc comment for the full
/// mechanism this table backs.
///
/// `impl PgExecutor`, the same "works on `&Pool` autocommit or inside a
/// caller's own open `Transaction`" treatment `ensure_idempotency_keys_table`
/// already gets, for the identical reason: `provision_bounded_context_schema`
/// above needs the latter (a brand-new bounded context), the
/// per-bounded-context startup loop in `skilj/src/lib.rs` needs the
/// former (patching a bounded context provisioned before this feature
/// existed). `CREATE TABLE IF NOT EXISTS`, called unconditionally on
/// every `build()`, is the whole migration story - a fresh table with no
/// existing rows needs no `ALTER TABLE` dance the way `idempotency_keys`'
/// own `client_id` retrofit did ([docs/architecture.md §37](../../../docs/architecture.md#idempotency-keys-client-id-scoping)).
#[tracing::instrument(skip_all)]
pub async fn ensure_external_message_cursors_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.external_message_cursors (
            adapter_id TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            last_sequence BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (adapter_id, partition_key)
        )"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// `projection_state.owner`/`projection_rebuild_state.owner` (cross-tenant
/// projection read fix, docs/architecture.md's own write-up of this
/// pass) - `ensure_idempotency_keys_table`'s own doc comment's "no
/// general per-bounded-context schema migration mechanism" applies
/// identically here, just for an added column rather than an added
/// table: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, called
/// unconditionally on every `build()`, is the whole migration story.
/// Unlike `ensure_idempotency_keys_table`, `provision_bounded_context_schema`
/// itself never needs this - a brand-new context's `CREATE TABLE`
/// already declares `owner` from the start - so this only ever runs
/// against an already-open `pool`, never inside that function's own
/// provisioning transaction, and takes `&Pool` directly rather than a
/// generic executor.
#[tracing::instrument(skip_all)]
pub async fn ensure_projection_state_owner_columns(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.projection_state ADD COLUMN IF NOT EXISTS owner TEXT"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.projection_rebuild_state ADD COLUMN IF NOT EXISTS owner TEXT"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `projection_state.as_of_sequence`/`projection_rebuild_state.as_of_sequence`
/// - the real fix behind Codeberg issue #25's investigation, following
/// `ensure_projection_state_owner_columns`'s own pattern exactly (see its
/// own doc comment): a bounded context provisioned before this column
/// existed gets it patched in here, `DEFAULT -1` matching a brand-new
/// row's own starting value.
///
/// A blanket `-1` backfill is safe even for a row with real accumulated
/// `state` from before this migration, and deliberately doesn't try to
/// derive each row's true historical position: `catch_up_bounded_context`/
/// `insert_event_and_update_sync_projections_in_tx` never re-fetch an
/// event once the *projection-level* `projections.caught_up_to` (an
/// existing column, untouched by this migration) has passed it - that
/// coarser gate is what actually decides which events a tick ever looks
/// at again, not this row's own `as_of_sequence`. So every event this
/// row's freshly-`-1` `as_of_sequence` will ever be compared against is
/// one `caught_up_to` hadn't reached yet at migration time, i.e. one this
/// row genuinely has never folded - the exact case `-1` is supposed to
/// mean.
#[tracing::instrument(skip_all)]
pub async fn ensure_projection_state_as_of_sequence_columns(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.projection_state \
         ADD COLUMN IF NOT EXISTS as_of_sequence BIGINT NOT NULL DEFAULT -1"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.projection_rebuild_state \
         ADD COLUMN IF NOT EXISTS as_of_sequence BIGINT NOT NULL DEFAULT -1"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `read_cursors.checked_out_at` - the real fix behind Codeberg issue
/// #25's investigation of the Kafka/AMQP/NATS outbound bridges
/// (docs/architecture.md §53): a bounded context provisioned before this
/// column existed gets it patched in here, following
/// `ensure_projection_state_owner_columns`'s own established pattern -
/// nullable, no `DEFAULT`, since `NULL` already means exactly "not
/// checked out" (`ReadCursor.checked_out_at`'s own spec doc comment),
/// the same value a brand-new row starts at.
pub async fn ensure_read_cursors_checkout_column(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.read_cursors ADD COLUMN IF NOT EXISTS checked_out_at TIMESTAMPTZ"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `event_types.owner_tag_key`/`access_tokens.scope`/
/// `command_types.owner_tag_key`/`snapshots.owner` - the raw-event,
/// command and snapshot halves of the cross-tenant read fix
/// (docs/architecture.md's own write-up of these passes), following
/// `ensure_projection_state_owner_columns`'s own pattern and reasoning
/// exactly (see its own doc comment): a targeted, idempotent `ALTER
/// TABLE ... ADD COLUMN IF NOT EXISTS` patch, called unconditionally on
/// every `build()`, for a bounded context provisioned before these
/// fields existed.
#[tracing::instrument(skip_all)]
pub async fn ensure_event_scoping_columns(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.event_types ADD COLUMN IF NOT EXISTS owner_tag_key TEXT"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.access_tokens ADD COLUMN IF NOT EXISTS scope TEXT"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.command_types ADD COLUMN IF NOT EXISTS owner_tag_key TEXT"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.snapshots ADD COLUMN IF NOT EXISTS owner TEXT"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `event_types.private_fields`/`command_types.private_fields` - the
/// private-field mechanism (docs/architecture.md's own write-up of this
/// pass), following `ensure_event_scoping_columns`'s own pattern and
/// reasoning exactly for a bounded context provisioned before these
/// columns existed.
pub async fn ensure_private_field_columns(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.event_types ADD COLUMN IF NOT EXISTS private_fields JSONB NOT NULL DEFAULT '[]'"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.command_types ADD COLUMN IF NOT EXISTS private_fields JSONB NOT NULL DEFAULT '[]'"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `commands.metadata_correlation_id`/`metadata_causation_id`,
/// `events.metadata_correlation_id`/`metadata_causation_id` (Codeberg
/// issue #18), following `ensure_event_scoping_columns`'s own pattern
/// and reasoning exactly for a bounded context provisioned before these
/// columns existed. The two partial indexes mirror
/// `provision_bounded_context_schema`'s own fresh-provision ones
/// (`events_by_correlation_id`/`commands_by_correlation_id`) - `CREATE
/// INDEX IF NOT EXISTS` rather than plain `CREATE INDEX`, since unlike
/// the columns themselves this runs unconditionally on every `build()`,
/// not only once.
#[tracing::instrument(skip_all)]
pub async fn ensure_correlation_causation_columns(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.events ADD COLUMN IF NOT EXISTS metadata_correlation_id TEXT"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.events ADD COLUMN IF NOT EXISTS metadata_causation_id TEXT"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.commands ADD COLUMN IF NOT EXISTS metadata_correlation_id TEXT"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.commands ADD COLUMN IF NOT EXISTS metadata_causation_id TEXT"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX IF NOT EXISTS events_by_correlation_id ON {schema}.events \
         (metadata_correlation_id) WHERE metadata_correlation_id IS NOT NULL"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX IF NOT EXISTS commands_by_correlation_id ON {schema}.commands \
         (metadata_correlation_id) WHERE metadata_correlation_id IS NOT NULL"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `access_tokens.start_from`/`.start_at_sequence`/`.start_at_time` -
/// `EventReadToken.start_from`/`.start_at_sequence`/`.start_at_time`
/// (docs/architecture.md's own write-up of these two passes), meaningful
/// only for the `event_read` kind but columns on the shared
/// `access_tokens` table exactly as `scope` already is
/// (`EventReadToken.scope`'s own doc comment on why one column serves
/// all four kinds). Following `ensure_event_scoping_columns`'s own
/// pattern exactly: targeted, idempotent `ALTER TABLE ... ADD COLUMN IF
/// NOT EXISTS`, called unconditionally on every `build()`, for a bounded
/// context provisioned before these columns existed - `start_at_sequence`/
/// `start_at_time` folded into this same function rather than a fourth
/// one, since they were added in the identical follow-up pass that
/// needs nothing about `start_from`'s own retrofit changed.
///
/// `start_from` is `NOT NULL DEFAULT 'beginning'` rather than nullable -
/// every row this backfills is a token minted before `start_from`
/// existed, and `EventReadStartPosition::Beginning` is exactly what such
/// a token already behaves as (`consume_events`' own `-1` fallback), so
/// the column's own default and the domain default agree;
/// `get_event_read_token` never has to handle an absent value. No such
/// domain default exists for `start_at_sequence`/`start_at_time` - a
/// `beginning` token never had one to fall back to - so both stay
/// nullable, exactly the shape `EventReadToken`'s own fields already
/// have.
pub async fn ensure_event_read_token_start_from_column(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.access_tokens \
         ADD COLUMN IF NOT EXISTS start_from TEXT NOT NULL DEFAULT 'beginning'"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.access_tokens ADD COLUMN IF NOT EXISTS start_at_sequence BIGINT"
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.access_tokens ADD COLUMN IF NOT EXISTS start_at_time TIMESTAMPTZ"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Physically, permanently removes a bounded context and everything
/// scoped to it - `DeleteBoundedContext` in specs/skilj.allium. One
/// transaction: `DROP SCHEMA ... CASCADE` (every per-context table this
/// module owns, gone in a single statement), then the `bounded_contexts`
/// registry row itself, whose own `ON DELETE CASCADE` cleans up
/// `role_access_mappings`/`access_token_index` automatically. The pure
/// `bootstrap::delete_bounded_context` check (superadmin, `archived`,
/// not `admin`) is the caller's job to run first - this function trusts
/// it already has.
#[tracing::instrument(skip_all, fields(name = %name))]
pub async fn hard_delete_bounded_context(pool: &Pool, name: &str) -> crate::error::Result<()> {
    let schema = schema_ident(name);
    let mut tx = pool.begin().await?;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM bounded_contexts WHERE name = $1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    notify_registration_changed(pool).await;
    Ok(())
}

// --- Role ---

#[derive(sqlx::FromRow)]
struct RoleRow {
    id: String,
    external_subject: String,
    name: String,
    superadmin: bool,
    status: String,
    created_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
}

impl RoleRow {
    fn into_domain(self) -> Role {
        Role {
            id: self.id,
            external_subject: self.external_subject,
            name: self.name,
            superadmin: self.superadmin,
            status: role_status_from_str(&self.status),
            created_at: self.created_at,
            revoked_at: self.revoked_at,
        }
    }
}

const ROLE_COLUMNS: &str = "id, external_subject, name, superadmin, status, created_at, revoked_at";

#[tracing::instrument(skip_all)]
pub async fn insert_role(pool: &Pool, role: &Role) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO roles ({ROLE_COLUMNS}) VALUES ($1,$2,$3,$4,$5,$6,$7)"
    )))
    .bind(&role.id)
    .bind(&role.external_subject)
    .bind(&role.name)
    .bind(role.superadmin)
    .bind(role_status_to_str(role.status))
    .bind(role.created_at)
    .bind(role.revoked_at)
    .execute(pool)
    .await?;
    Ok(())
}

#[tracing::instrument(skip_all)]
pub async fn get_role(pool: &Pool, id: &str) -> crate::error::Result<Option<Role>> {
    let row: Option<RoleRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ROLE_COLUMNS} FROM roles WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(RoleRow::into_domain))
}

/// Every `Role` this engine currently knows of - the full-snapshot
/// parameter `create_role`/`resolve_role_by_external_subject` each
/// expect as their own `existing_roles` (see their doc comments). Small
/// and admin-managed, unlike `Event`/`Command`, so an unscoped list is
/// the right shape here - no per-bounded-context narrowing to do, since
/// a `Role` isn't scoped to one.
#[tracing::instrument(skip_all)]
pub async fn list_roles(pool: &Pool) -> crate::error::Result<Vec<Role>> {
    let rows: Vec<RoleRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ROLE_COLUMNS} FROM roles"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(RoleRow::into_domain).collect())
}

/// Persists whatever `revoke_role`/`create_superadmin` (or any future
/// rule) produced - a full-row overwrite by `id`, not an upsert; the row
/// must already exist (`insert_role` already ran). `impl PgExecutor`,
/// the same "works on `&Pool` autocommit or inside a caller's own open
/// `Transaction`" treatment `next_sequence` already gets - `revoke_role_and_mappings`
/// below is what needs the latter.
#[tracing::instrument(skip_all)]
pub async fn update_role<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    role: &Role,
) -> crate::error::Result<()> {
    sqlx::query(
        "UPDATE roles SET external_subject = $1, name = $2, superadmin = $3, status = $4, \
         created_at = $5, revoked_at = $6 WHERE id = $7",
    )
    .bind(&role.external_subject)
    .bind(&role.name)
    .bind(role.superadmin)
    .bind(role_status_to_str(role.status))
    .bind(role.created_at)
    .bind(role.revoked_at)
    .bind(&role.id)
    .execute(executor)
    .await?;
    Ok(())
}

// --- BoundedContext ---

#[derive(sqlx::FromRow)]
struct BoundedContextRow {
    name: String,
    status: String,
    created_at: DateTime<Utc>,
    created_by_kind: String,
    created_by_role_id: Option<String>,
    /// Codeberg issue #13 - just the referenced row's own name; hydrated
    /// into a full `BoundedContext` by `get_bounded_context`/
    /// `list_bounded_contexts`, matching how `created_by_role_id` is
    /// hydrated into a full `Role` below.
    template: Option<String>,
}

const BOUNDED_CONTEXT_COLUMNS: &str =
    "name, status, created_at, created_by_kind, created_by_role_id, template";

/// Provisions the new context's own `bc_<name>` schema (see
/// `provision_bounded_context_schema`) and inserts its `bounded_contexts`
/// registry row in one transaction - a failure partway through either
/// half rolls back the other, so there's never an orphaned schema with
/// no registry row or vice versa. Callers (test fixtures included) don't
/// need to know any of this happens; the signature is unchanged from
/// before schema-per-context existed.
#[tracing::instrument(skip_all)]
pub async fn insert_bounded_context(pool: &Pool, bc: &BoundedContext) -> crate::error::Result<()> {
    let (kind, role_id) = match &bc.created_by {
        ContextCreator::SystemCreator => ("system", None),
        ContextCreator::SuperadminCreator { role } => ("superadmin", Some(role.id.clone())),
    };

    let mut tx = pool.begin().await?;
    provision_bounded_context_schema(&mut tx, &bc.name).await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO bounded_contexts ({BOUNDED_CONTEXT_COLUMNS}) VALUES ($1,$2,$3,$4,$5,$6)"
    )))
    .bind(&bc.name)
    .bind(bounded_context_status_to_str(bc.status))
    .bind(bc.created_at)
    .bind(kind)
    .bind(role_id)
    .bind(bc.template.as_ref().map(|t| t.name.clone()))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    notify_registration_changed(pool).await;
    Ok(())
}

/// Persists an `archive_bounded_context` outcome - a status-only update,
/// the only field any rule in this crate ever changes on an existing
/// `bounded_contexts` row (`name`/`created_at`/`created_by` are fixed at
/// creation - `rule AddBoundedContext`'s own "chosen once, at the moment
/// of creation" framing, and there is no rename). Distinct from
/// `insert_bounded_context`, which provisions a brand new schema - this
/// touches only the registry row of one that already exists.
#[tracing::instrument(skip_all, fields(name = %name))]
pub async fn update_bounded_context_status(
    pool: &Pool,
    name: &str,
    status: BoundedContextStatus,
) -> crate::error::Result<()> {
    sqlx::query("UPDATE bounded_contexts SET status = $1 WHERE name = $2")
        .bind(bounded_context_status_to_str(status))
        .bind(name)
        .execute(pool)
        .await?;
    notify_registration_changed(pool).await;
    Ok(())
}

/// Unlike most `into_domain`-style conversions in this module,
/// `ContextCreator::SuperadminCreator` needs a second query (the
/// referenced `roles` row - see the migration's own note on why
/// `bounded_contexts` normalises onto `roles` rather than denormalising
/// its fields directly), so this is a free function taking the row plus
/// an already-resolved `Option<Role>`, not a method on the row type.
fn bounded_context_from_row(
    row: BoundedContextRow,
    role: Option<Role>,
    template: Option<Box<BoundedContext>>,
) -> BoundedContext {
    let created_by = match row.created_by_kind.as_str() {
        "system" => ContextCreator::SystemCreator,
        _ => ContextCreator::SuperadminCreator {
            role: role.expect(
                "bounded_contexts row has created_by_kind = 'superadmin' but its \
                 created_by_role_id doesn't resolve to a roles row - the FK should make \
                 this unreachable",
            ),
        },
    };
    BoundedContext {
        name: row.name,
        status: bounded_context_status_from_str(&row.status),
        created_at: row.created_at,
        created_by,
        template,
    }
}

/// Like `get_bounded_context`, for callers that already hold a name they
/// expect to exist. A missing row still comes back as an ordinary
/// `sqlx::Error::RowNotFound` rather than a panic: the caller's own access
/// check can pass just before a concurrent `hard_delete_bounded_context`
/// lands, and that race must surface as an error response, not take the
/// request task down.
async fn require_bounded_context(pool: &Pool, name: &str) -> crate::error::Result<BoundedContext> {
    get_bounded_context(pool, name)
        .await?
        .ok_or_else(|| sqlx::Error::RowNotFound.into())
}

/// Like [`require_bounded_context`], for an `EventType` a caller already
/// has a stored reference to (an `events`/`access_tokens` row's own
/// `event_type_name`) - the identical `hard_delete_bounded_context` race
/// applies: the whole schema, `event_types` included, can vanish between
/// an earlier query in the same function and this one.
async fn require_event_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<EventType> {
    get_event_type(pool, bounded_context, name)
        .await?
        .ok_or_else(|| sqlx::Error::RowNotFound.into())
}

/// [`require_event_type`]'s `CommandType` sibling.
async fn require_command_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<CommandType> {
    get_command_type(pool, bounded_context, name)
        .await?
        .ok_or_else(|| sqlx::Error::RowNotFound.into())
}

/// [`require_event_type`]'s `Command` sibling, for an `events` row's own
/// `origin_command_id`.
async fn require_command(
    pool: &Pool,
    bounded_context: &str,
    id: i64,
) -> crate::error::Result<Command> {
    get_command_by_id(pool, bounded_context, id)
        .await?
        .ok_or_else(|| sqlx::Error::RowNotFound.into())
}

#[tracing::instrument(skip_all, fields(name = %name))]
pub async fn get_bounded_context(
    pool: &Pool,
    name: &str,
) -> crate::error::Result<Option<BoundedContext>> {
    let Some(row): Option<BoundedContextRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {BOUNDED_CONTEXT_COLUMNS} FROM bounded_contexts WHERE name = $1"
    )))
    .bind(name)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };

    let role = match &row.created_by_role_id {
        Some(role_id) => Some(get_role(pool, role_id).await?.expect(
            "bounded_contexts.created_by_role_id references a roles row that no longer exists",
        )),
        None => None,
    };
    // Invariant TemplateIsNeverItselfTemplated: a template's own `template`
    // is always `None`, so this recursion is at most one level deep - it
    // can never loop.
    //
    // Ultra-review bug_006: this template row and the tenant row above
    // are two separate `SELECT`s, no shared transaction - a real,
    // legitimate race (the template gets deleted, and its own
    // `ON DELETE SET NULL` cascade fires, between the two) means this
    // recursive lookup can genuinely return `None` even though the
    // outer row's own snapshot still said `Some(template_name)`. Treated
    // as `None` here rather than an `.expect()` panic: that's exactly
    // the state a fresh re-read of the same tenant would show anyway,
    // once the delete has committed.
    let template = match &row.template {
        Some(template_name) => Box::pin(get_bounded_context(pool, template_name))
            .await?
            .map(Box::new),
        None => None,
    };
    Ok(Some(bounded_context_from_row(row, role, template)))
}

/// Every `BoundedContext` this engine currently knows of - the
/// `bounded_contexts` parameter `bootstrap::list_bounded_contexts`
/// expects (see its own doc comment: unrestricted, same full-snapshot
/// treatment `list_roles`/`list_role_access_mappings` get). Backs the
/// `BoundedContextDirectory` surface's `boundedContexts` query - and,
/// Codeberg issue #15, `SkiljBuilder::build()`'s startup warm-up loop
/// plus all three background pollers, which is why this used to be a
/// real N+1 (one `get_role` query per row) worth fixing: `list_roles`
/// already fetches every `Role` in one query, so one full-table fetch
/// plus an in-memory lookup replaces what was one query per row.
#[tracing::instrument(skip_all)]
pub async fn list_bounded_contexts(pool: &Pool) -> crate::error::Result<Vec<BoundedContext>> {
    let rows: Vec<BoundedContextRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {BOUNDED_CONTEXT_COLUMNS} FROM bounded_contexts"
    )))
    .fetch_all(pool)
    .await?;

    let roles_by_id: std::collections::HashMap<String, Role> = list_roles(pool)
        .await?
        .into_iter()
        .map(|role| (role.id.clone(), role))
        .collect();

    // Codeberg issue #13: a template is always some other row already in
    // this same full-table fetch (invariant TemplateIsNeverItselfTemplated
    // rules out a template needing a template of its own), so resolving
    // `template` here is a map lookup, not an extra query per row - the
    // same N+1 avoidance this function's own doc comment already commits
    // to for `created_by_role_id`/`Role`. Build every context with
    // `template: None` first, keyed by name, then fill in the box on a
    // second pass.
    let mut contexts_by_name = std::collections::HashMap::with_capacity(rows.len());
    let mut template_names = Vec::with_capacity(rows.len());
    for row in rows {
        let role = row.created_by_role_id.as_ref().map(|role_id| {
            roles_by_id.get(role_id).cloned().expect(
                "bounded_contexts.created_by_role_id references a roles row that no longer \
                 exists",
            )
        });
        let name = row.name.clone();
        let template_name = row.template.clone();
        contexts_by_name.insert(name.clone(), bounded_context_from_row(row, role, None));
        template_names.push((name, template_name));
    }
    for (name, template_name) in template_names {
        let Some(template_name) = template_name else {
            continue;
        };
        let template = contexts_by_name.get(&template_name).cloned().expect(
            "bounded_contexts.template references a bounded_contexts row that no longer exists",
        );
        contexts_by_name.get_mut(&name).unwrap().template = Some(Box::new(template));
    }

    // Order isn't part of this function's contract (see callers - a
    // `HashMap`-keyed round trip like this one loses whatever order the
    // query returned), matching `list_roles`/`list_role_access_mappings`'s
    // own unordered `Vec` return.
    Ok(contexts_by_name.into_values().collect())
}

/// Ultra-review bug_005's own fix - see migration `0003_add_bounded_
/// context_dispatch_template.sql`'s own doc comment for the full
/// reasoning. `createBoundedContextFromTemplate` calls this once,
/// immediately after `insert_bounded_context` succeeds, to set the
/// permanent, never-cleared record `TemplateCache`'s dispatch resolution
/// reads from (`list_dispatch_template_mappings` below) - deliberately
/// not part of `insert_bounded_context`'s own signature, since every
/// other caller of that function (bootstrap, ordinary `AddBoundedContext`,
/// every test fixture) has no dispatch template to set and shouldn't
/// need to pass one.
#[tracing::instrument(skip_all, fields(name = %name))]
pub async fn set_dispatch_template(
    pool: &Pool,
    name: &str,
    dispatch_template: &str,
) -> crate::error::Result<()> {
    sqlx::query("UPDATE bounded_contexts SET dispatch_template = $1 WHERE name = $2")
        .bind(dispatch_template)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
}

/// `TemplateCache::refresh`'s own data source - every `BoundedContext`'s
/// own `name` paired with its `dispatch_template` (`None` for the
/// ordinary, untemplated case), read directly rather than through
/// `list_bounded_contexts`'s full `BoundedContext` hydration: dispatch
/// resolution needs nothing else about a context, and this stays a
/// single flat query with no recursive template lookup, no `Role`
/// join - a smaller, cheaper, single-purpose read for a call this
/// crate's own cross-instance listener makes on every registration
/// change.
#[tracing::instrument(skip_all)]
pub async fn list_dispatch_template_mappings(
    pool: &Pool,
) -> crate::error::Result<Vec<(String, Option<String>)>> {
    let rows: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT name, dispatch_template FROM bounded_contexts")
            .fetch_all(pool)
            .await?;
    Ok(rows)
}

// --- EventType ---

fn missed_occurrence_policy_to_str(policy: MissedOccurrencePolicy) -> &'static str {
    match policy {
        MissedOccurrencePolicy::Skip => "skip",
        MissedOccurrencePolicy::FireOnce => "fire_once",
        MissedOccurrencePolicy::ReplayBacklog => "replay_backlog",
    }
}

fn missed_occurrence_policy_from_str(s: &str) -> MissedOccurrencePolicy {
    match s {
        "fire_once" => MissedOccurrencePolicy::FireOnce,
        "replay_backlog" => MissedOccurrencePolicy::ReplayBacklog,
        _ => MissedOccurrencePolicy::Skip,
    }
}

#[derive(sqlx::FromRow)]
struct EventTypeRow {
    name: String,
    schema: String,
    schema_version: i64,
    tag_mappings: Json<Vec<TagMapping>>,
    owner_tag_key: Option<String>,
    sensitive_fields: Json<Vec<SensitiveField>>,
    external_creation_allowed: bool,
    direct_creation_allowed: bool,
    system_triggered_allowed: bool,
    system_triggered_schedule: Option<String>,
    missed_occurrence_policy: Option<String>,
    schedule_position: Option<DateTime<Utc>>,
    last_fired_at: Option<DateTime<Utc>>,
    event_read_allowed: bool,
    private_fields: Json<Vec<PrivateField>>,
}

impl EventTypeRow {
    fn into_domain(self, bounded_context: BoundedContext) -> EventType {
        EventType {
            bounded_context,
            name: self.name,
            schema: self.schema,
            schema_version: self.schema_version,
            tag_mappings: self.tag_mappings.0,
            owner_tag_key: self.owner_tag_key,
            sensitive_fields: self.sensitive_fields.0,
            private_fields: self.private_fields.0,
            external_creation_allowed: self.external_creation_allowed,
            direct_creation_allowed: self.direct_creation_allowed,
            system_triggered_allowed: self.system_triggered_allowed,
            system_triggered_schedule: self.system_triggered_schedule,
            missed_occurrence_policy: self
                .missed_occurrence_policy
                .as_deref()
                .map(missed_occurrence_policy_from_str),
            schedule_position: self.schedule_position,
            last_fired_at: self.last_fired_at,
            event_read_allowed: self.event_read_allowed,
        }
    }
}

const EVENT_TYPE_COLUMNS: &str =
    "name, schema, schema_version, tag_mappings, owner_tag_key, sensitive_fields, \
    external_creation_allowed, direct_creation_allowed, system_triggered_allowed, \
    system_triggered_schedule, missed_occurrence_policy, schedule_position, last_fired_at, \
    event_read_allowed, private_fields";

/// Upsert, not insert-only - `RegisterEventType`'s own create-or-update
/// shape (see `event_store::register_event_type`), though no surface
/// calls this yet this pass; used directly by tests/seeding until
/// `RegisterEventType` itself has a GraphQL route in front of it.
#[tracing::instrument(skip_all)]
pub async fn upsert_event_type(pool: &Pool, et: &EventType) -> crate::error::Result<()> {
    let schema = schema_ident(&et.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.event_types ({EVENT_TYPE_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
         ON CONFLICT (name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            tag_mappings = EXCLUDED.tag_mappings, owner_tag_key = EXCLUDED.owner_tag_key, \
            sensitive_fields = EXCLUDED.sensitive_fields, \
            external_creation_allowed = EXCLUDED.external_creation_allowed, \
            direct_creation_allowed = EXCLUDED.direct_creation_allowed, \
            system_triggered_allowed = EXCLUDED.system_triggered_allowed, \
            system_triggered_schedule = EXCLUDED.system_triggered_schedule, \
            missed_occurrence_policy = EXCLUDED.missed_occurrence_policy, \
            schedule_position = EXCLUDED.schedule_position, \
            last_fired_at = EXCLUDED.last_fired_at, \
            event_read_allowed = EXCLUDED.event_read_allowed, \
            private_fields = EXCLUDED.private_fields"
    )))
    .bind(&et.name)
    .bind(&et.schema)
    .bind(et.schema_version)
    .bind(Json(&et.tag_mappings))
    .bind(&et.owner_tag_key)
    .bind(Json(&et.sensitive_fields))
    .bind(et.external_creation_allowed)
    .bind(et.direct_creation_allowed)
    .bind(et.system_triggered_allowed)
    .bind(&et.system_triggered_schedule)
    .bind(
        et.missed_occurrence_policy
            .map(missed_occurrence_policy_to_str),
    )
    .bind(et.schedule_position)
    .bind(et.last_fired_at)
    .bind(et.event_read_allowed)
    .bind(Json(&et.private_fields))
    .execute(pool)
    .await?;
    notify_registration_changed(pool).await;
    Ok(())
}

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_event_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<EventType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    get_event_type_with_bc(pool, &bc, name).await
}

/// [`get_event_type`]'s own `bc`-in-hand sibling (Codeberg issue #32,
/// round four - same precedent §60 already set for
/// `list_events_for_bounded_context_matching_tags_with_bc`): skips the
/// `get_bounded_context` call entirely for a caller who already has the
/// row - `list_events_for_bounded_context_matching_tags_with_bc`'s own
/// row-processing loop, one call per distinct event type in a delta
/// result, is exactly the caller this was added for - measured (a real
/// load test, not just reading the code) spending the majority of its
/// own wall-clock time re-fetching a `BoundedContext` (plus, transitively,
/// its own `created_by_role_id`'s `Role`) it never needed to ask for
/// again.
async fn get_event_type_with_bc(
    pool: &Pool,
    bc: &BoundedContext,
    name: &str,
) -> crate::error::Result<Option<EventType>> {
    let schema = schema_ident(&bc.name);
    let row: Option<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = $1"
    )))
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.into_domain(bc.clone())))
}

/// `get_event_type_with_bc`'s batched sibling - `get_command_types_by_names_with_bc`'s
/// own precedent, one `WHERE name = ANY($1)` round trip for a whole set
/// of names. `names` empty returns an empty map without touching
/// Postgres.
async fn get_event_types_by_names_with_bc(
    pool: &Pool,
    bc: &BoundedContext,
    names: &[&str],
) -> crate::error::Result<std::collections::HashMap<String, EventType>> {
    if names.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let schema = schema_ident(&bc.name);
    let rows: Vec<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = ANY($1)"
    )))
    .bind(names)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.name.clone(), row.into_domain(bc.clone())))
        .collect())
}

/// Every `EventType` in `bounded_context` currently opted into
/// scheduling (`system_triggered_allowed = true`) - the background
/// scheduler's own discovery query each tick, and `TypeRegistration`'s
/// `scheduledEventTypes` GraphQL query (`@guarantee ScheduleStateIsShared`'s
/// own admin visibility) both read from this. Ordered by `name` for
/// stable output. `[]`, not an error, for an unknown `bounded_context` -
/// the same "nothing to show" treatment `list_projections_for_bounded_context`
/// gives it.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_scheduled_event_types(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<EventType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types \
         WHERE system_triggered_allowed = true ORDER BY name"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.into_domain(bc.clone()))
        .collect())
}

/// Every `EventType` registered in `bounded_context`, unfiltered -
/// `TypeRegistration`'s own `eventTypes` GraphQL query (Codeberg issue
/// #6's "5a": the self-describing surface `skilj-tui`'s Commands/Query
/// Events tabs need to offer a real type picker instead of a name typed
/// by hand). Same shape as `list_scheduled_event_types` immediately
/// above, minus its `WHERE system_triggered_allowed = true` filter -
/// every type, not just the scheduled subset. Ordered by `name`, `[]`
/// for an unknown `bounded_context`, same reasoning as its sibling.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_event_types_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<EventType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types ORDER BY name"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.into_domain(bc.clone()))
        .collect())
}

/// `CommandType`'s own equivalent of `list_event_types_for_bounded_context`
/// immediately above - identical reasoning, `TypeRegistration`'s
/// `commandTypes` GraphQL query.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_command_types_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<CommandType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<CommandTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_TYPE_COLUMNS} FROM {schema}.command_types ORDER BY name"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.into_domain(bc.clone()))
        .collect())
}

/// `rule CreateSystemEvent`'s own atomic whole, and `SequenceIsGaplessPerBoundedContext`'s
/// enforcement for a fired occurrence - locks `event_type_name`'s own
/// `event_types` row (`SELECT ... FOR UPDATE`), re-derives eligibility
/// against the now-locked, up-to-date `schedule_position`/`last_fired_at`
/// (another instance may have already advanced them since the caller's
/// own `list_scheduled_event_types` read - see `@guarantee
/// ScheduleStateIsShared`), and only then allocates a sequence number and
/// inserts - the same "peek, lock, re-check, single commit" shape
/// `submit_command` uses for `bounded_context.sequence`, adapted to one
/// `event_types` row instead. `event_dispatcher.scheduled_payload` is
/// resolved *before* the lock, deliberately: it's arbitrary caller code
/// (unlike `decide()`/`project()`, §1.1 never promised it's cheap or
/// I/O-free), so it must never run while this row's lock is held - that
/// would block every other writer to this event type for however long it
/// takes. `Ok(None)` when there is nothing to fire: the occurrence turned
/// out to be ineligible once the lock confirmed the fresh position (not
/// an error - exactly the race this locking exists to make harmless), or
/// this process's own `EventDispatcher` has no `scheduled_payload`
/// producer registered for the type at all (a genuine misconfiguration,
/// tolerated the same way `catch_up_bounded_context` tolerates an
/// unregistered projection dispatcher - logged and skipped by the
/// caller, not propagated as an `Err`).
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn fire_system_event(
    pool: &Pool,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    event_dispatcher: &dyn crate::plugin::EventDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    event_type_name: &str,
    occurrence_at: DateTime<Utc>,
    now: DateTime<Utc>,
    encryption_master_key: Option<&EncryptionMasterKey>,
) -> crate::error::Result<Option<Event>> {
    let Some(unlocked_event_type) = get_event_type(pool, bounded_context, event_type_name).await?
    else {
        return Ok(None);
    };
    let Some(payload) = event_dispatcher.scheduled_payload(bounded_context, event_type_name) else {
        return Ok(None);
    };

    let mut resolved = std::collections::HashMap::new();
    resolve_encryption_keys(
        pool,
        bounded_context,
        &unlocked_event_type.sensitive_fields,
        &payload,
        encryption_master_key,
        &mut resolved,
    )
    .await?;

    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;
    let row: Option<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = $1 FOR UPDATE"
    )))
    .bind(event_type_name)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let event_type = row.into_domain(unlocked_event_type.bounded_context);

    // Allocated inside `tx`, after the lock above - if `create_system_event`
    // below rejects the occurrence, `tx` is dropped without committing
    // (the early `return Ok(None)`), rolling this allocation back too, so
    // no sequence number is ever burned on an occurrence that didn't
    // actually fire.
    let next_seq = next_sequence(&mut *tx, bounded_context).await?;
    let Some((event, new_position)) = crate::event_store::create_system_event(
        &event_type,
        occurrence_at,
        now,
        next_seq,
        || payload,
        |subject_key, subject_value| {
            let (key, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect(
                    "resolve_encryption_keys pre-resolved every subject sensitive_field_subjects \
                     named",
                );
            (key.clone(), data_key.clone())
        },
    ) else {
        return Ok(None);
    };

    let encryption_key_ids = encryption_key_ids(&event.encryption_keys, &resolved);
    // Fetched only now - after `next_sequence` above already took this
    // bounded context's own lock - see `insert_event_and_update_sync_projections_in_tx`'s
    // own doc comment on why that ordering, not "as early as possible", is
    // what keeps this read race-free against `promote_projection_rebuild`.
    let sync_projections = sync_projections_for_bounded_context(pool, bounded_context).await?;
    insert_event_and_update_sync_projections_in_tx(
        &mut tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
        &sync_projections,
    )
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.event_types SET schedule_position = $1, last_fired_at = $2 WHERE name = $3"
    )))
    .bind(new_position)
    .bind(new_position)
    .bind(event_type_name)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    broadcaster.publish(&event);
    record_event_appended(&event);
    notify_event_appended(pool, &event, broadcaster.instance_id()).await;
    event_cache.append(&event).await;

    Ok(Some(event))
}

/// `rule SkipMissedOccurrences`'s own atomic whole - locks the same
/// `event_types` row `fire_system_event` does, re-derives eligibility
/// against the locked, up-to-date `schedule_position` (a second instance
/// resuming alongside the first is exactly the harmless race this lock
/// makes a no-op - see `event_store::skip_missed_occurrences`'s own doc
/// comment), and persists the advanced position alone: unlike
/// `fire_system_event`, no event is produced and `last_fired_at` never
/// moves (`FiredOccurrenceIsAccountedFor` only ever relates it to
/// occurrences that actually fired). `Ok(None)` when the resume turns out
/// to be a no-op once the lock is held.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn skip_missed_occurrences_for_event_type(
    pool: &Pool,
    bounded_context: &str,
    event_type_name: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<Option<DateTime<Utc>>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;
    let row: Option<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = $1 FOR UPDATE"
    )))
    .bind(event_type_name)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let event_type = row.into_domain(bc);

    let Some(new_position) = crate::event_store::skip_missed_occurrences(&event_type, now) else {
        return Ok(None);
    };

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.event_types SET schedule_position = $1 WHERE name = $2"
    )))
    .bind(new_position)
    .bind(event_type_name)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(Some(new_position))
}

// --- CommandType ---

#[derive(sqlx::FromRow)]
struct CommandTypeRow {
    name: String,
    schema: String,
    schema_version: i64,
    tag_mappings: Json<Vec<TagMapping>>,
    owner_tag_key: Option<String>,
    sensitive_fields: Json<Vec<SensitiveField>>,
    rest_trigger_allowed: bool,
    private_fields: Json<Vec<PrivateField>>,
}

impl CommandTypeRow {
    fn into_domain(self, bounded_context: BoundedContext) -> CommandType {
        CommandType {
            bounded_context,
            name: self.name,
            schema: self.schema,
            schema_version: self.schema_version,
            tag_mappings: self.tag_mappings.0,
            owner_tag_key: self.owner_tag_key,
            sensitive_fields: self.sensitive_fields.0,
            private_fields: self.private_fields.0,
            rest_trigger_allowed: self.rest_trigger_allowed,
        }
    }
}

const COMMAND_TYPE_COLUMNS: &str = "name, schema, schema_version, tag_mappings, owner_tag_key, \
    sensitive_fields, rest_trigger_allowed, private_fields";

/// See `upsert_event_type` above - same shape and reasoning.
#[tracing::instrument(skip_all)]
pub async fn upsert_command_type(pool: &Pool, ct: &CommandType) -> crate::error::Result<()> {
    let schema = schema_ident(&ct.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.command_types ({COMMAND_TYPE_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) \
         ON CONFLICT (name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            tag_mappings = EXCLUDED.tag_mappings, owner_tag_key = EXCLUDED.owner_tag_key, \
            sensitive_fields = EXCLUDED.sensitive_fields, \
            rest_trigger_allowed = EXCLUDED.rest_trigger_allowed, \
            private_fields = EXCLUDED.private_fields"
    )))
    .bind(&ct.name)
    .bind(&ct.schema)
    .bind(ct.schema_version)
    .bind(Json(&ct.tag_mappings))
    .bind(&ct.owner_tag_key)
    .bind(Json(&ct.sensitive_fields))
    .bind(ct.rest_trigger_allowed)
    .bind(Json(&ct.private_fields))
    .execute(pool)
    .await?;
    notify_registration_changed(pool).await;
    Ok(())
}

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_command_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<CommandType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let row: Option<CommandTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_TYPE_COLUMNS} FROM {schema}.command_types WHERE name = $1"
    )))
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.into_domain(bc)))
}

// --- EncryptionKey ---

fn encryption_key_status_from_str(s: &str) -> EncryptionKeyStatus {
    match s {
        "destroyed" => EncryptionKeyStatus::Destroyed,
        _ => EncryptionKeyStatus::Active,
    }
}

#[derive(sqlx::FromRow)]
struct EncryptionKeyRow {
    id: i64,
    subject_key: String,
    subject_value: String,
    status: String,
    created_at: DateTime<Utc>,
    destroyed_at: Option<DateTime<Utc>>,
    wrapped_key: Option<Vec<u8>>,
    wrap_nonce: Option<Vec<u8>>,
}

impl EncryptionKeyRow {
    fn to_domain(&self, bounded_context: BoundedContext) -> EncryptionKey {
        EncryptionKey {
            bounded_context,
            subject_key: self.subject_key.clone(),
            subject_value: self.subject_value.clone(),
            status: encryption_key_status_from_str(&self.status),
            created_at: self.created_at,
            destroyed_at: self.destroyed_at,
        }
    }
}

const ENCRYPTION_KEY_COLUMNS: &str =
    "id, subject_key, subject_value, status, created_at, destroyed_at, wrapped_key, wrap_nonce";

/// `event_store::protect_sensitive_fields`'s own get-or-create - "each key
/// provisioned exactly once... idempotent... repeat calls find the key
/// already provisioned rather than creating another one" (the note above
/// `rule CreateExternalEvent`). Not literally inside the same transaction
/// as the event/command write it protects - reuses the same "pre-resolve,
/// then inject as a plain closure" shape `next_sequence`'s own callers
/// already use for the identical "pure core needs a value only I/O can
/// produce" problem (see docs/architecture.md's write-up of this pass).
/// `ON CONFLICT ... DO NOTHING RETURNING` first, falling back to a plain
/// `SELECT` on the race-lost case - the standard upsert-or-fetch idiom,
/// safe under `encryption_keys_unique_active`'s own partial unique index.
///
/// Returns the row's own synthetic `id` alongside the domain entity (which
/// has none, matching `entity EncryptionKey` itself) and the unwrapped
/// `DataKey` - the same "domain struct has no id, caller gets one back
/// anyway for FK purposes" treatment `insert_command`'s own return value
/// already has. The caller needs `id` to link `event_encryption_keys`/
/// `command_encryption_keys` join rows once the event/command itself is
/// inserted.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_or_create_encryption_key(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    master_key: &EncryptionMasterKey,
) -> crate::error::Result<(EncryptionKey, i64, DataKey)> {
    let bc = require_bounded_context(pool, bounded_context).await?;
    let schema = schema_ident(bounded_context);

    if let Some(row) =
        get_active_encryption_key_row(pool, bounded_context, subject_key, subject_value).await?
    {
        let data_key = unwrap_row(master_key, &row)?;
        return Ok((row.to_domain(bc), row.id, data_key));
    }

    let (data_key, wrapped, nonce) = encryption::generate_and_wrap_data_key(master_key);
    let now = Utc::now();
    let inserted: Option<EncryptionKeyRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.encryption_keys \
         (subject_key, subject_value, status, created_at, wrapped_key, wrap_nonce) \
         VALUES ($1, $2, 'active', $3, $4, $5) \
         ON CONFLICT (subject_key, subject_value) WHERE status = 'active' DO NOTHING \
         RETURNING {ENCRYPTION_KEY_COLUMNS}"
    )))
    .bind(subject_key)
    .bind(subject_value)
    .bind(now)
    .bind(&wrapped)
    .bind(&nonce)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = inserted {
        return Ok((row.to_domain(bc), row.id, data_key));
    }

    // Lost the race to a concurrent provisioner - the row it created is
    // now the active one; re-fetch and unwrap that instead of the key
    // just generated above (which was never persisted, so it must not be
    // used - the two would silently disagree on later re-reads).
    let row = get_active_encryption_key_row(pool, bounded_context, subject_key, subject_value)
        .await?
        .expect(
            "get_or_create_encryption_key: INSERT lost the race but no active row was found \
             immediately after - a concurrent provisioner must have destroyed it in between, \
             which the active-only unique index makes vanishingly unlikely within one call",
        );
    let data_key = unwrap_row(master_key, &row)?;
    Ok((row.to_domain(bc), row.id, data_key))
}

fn unwrap_row(
    master_key: &EncryptionMasterKey,
    row: &EncryptionKeyRow,
) -> crate::error::Result<DataKey> {
    let wrapped = row
        .wrapped_key
        .as_deref()
        .expect("an active encryption_keys row always still has its wrapped key");
    let nonce = row
        .wrap_nonce
        .as_deref()
        .expect("an active encryption_keys row always still has its wrap nonce");
    Ok(encryption::unwrap_data_key(master_key, wrapped, nonce)?)
}

/// The pre-resolution step every `event_store::protect_sensitive_fields`
/// caller needs before it can call that pure function at all - see
/// `docs/architecture.md`'s write-up of this pass and
/// `event_store::sensitive_field_subjects`'s own doc comment. Resolves
/// (get-or-creates) every `EncryptionKey` `payload` will need, merging
/// into `resolved` rather than returning a fresh map - a caller
/// protecting more than one payload against the same subject (a command
/// and the events it produces, in `process_command`'s case) calls this
/// once per payload, accumulating into the same map, so a shared subject
/// resolves to the identical `EncryptionKey` everywhere, exactly as the
/// spec requires. A no-op, no `master_key` needed, when `sensitive_fields`
/// is empty or names no subjects actually present in `payload` - the
/// identical "empty is a no-op" register `protect_sensitive_fields`
/// itself is in. `master_key: None` with something to resolve is a real,
/// actionable configuration error, not a silent skip.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn resolve_encryption_keys(
    pool: &Pool,
    bounded_context: &str,
    sensitive_fields: &[SensitiveField],
    payload: &str,
    master_key: Option<&EncryptionMasterKey>,
    resolved: &mut std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
) -> crate::error::Result<()> {
    let subjects = crate::event_store::sensitive_field_subjects(sensitive_fields, payload);
    if subjects.is_empty() {
        return Ok(());
    }
    let master_key = master_key.ok_or(encryption::Error::MasterKeyNotConfigured)?;
    for (subject_key, subject_value) in subjects {
        if resolved.contains_key(&(subject_key.clone(), subject_value.clone())) {
            continue;
        }
        let provisioned = get_or_create_encryption_key(
            pool,
            bounded_context,
            &subject_key,
            &subject_value,
            master_key,
        )
        .await?;
        resolved.insert((subject_key, subject_value), provisioned);
    }
    Ok(())
}

/// The read-side twin of `resolve_encryption_keys` above - the
/// pre-resolution step `event_store::render_event`/`render_command`'s own
/// caller needs before it can call either pure function at all. Same
/// accumulator-across-a-batch shape (a subject shared across many
/// events/commands in one query resolves once, not once per row) -
/// `resolvers::event_query`/`command_query`/`event_subscription` each
/// call this once per event/command in the batch being rendered,
/// accumulating into the same map.
///
/// Unlike the write side, this **filters by
/// `event_store::sensitive_field_is_granted` first** - only a subject
/// this caller is actually granted decrypt access to is ever looked up,
/// so an unauthorised caller's query needs no `master_key` at all, no
/// matter how many sensitive fields the payload declares. `master_key:
/// None` with at least one *granted* subject to resolve is a real,
/// actionable configuration error (confirmed via `AskUserQuestion` before
/// building - mirrors `resolve_encryption_keys`'s own identical
/// precedent) - a caller who *is* entitled to see a decrypted value
/// deserves to know the server can't produce one, not silent ciphertext
/// indistinguishable from "you're not authorised." A subject that *is*
/// granted but has no active key (destroyed by `ForgetSubject`, or never
/// provisioned) simply isn't inserted into `resolved` - `render_event`/
/// `render_command`'s own `resolve_data_key` closure sees `None` for it,
/// the correct crypto-shredding outcome, not an error.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn resolve_data_keys_for_reading(
    pool: &Pool,
    bounded_context: &str,
    sensitive_fields: &[SensitiveField],
    payload: &str,
    access_mapping: &crate::access_control::RoleAccessMapping,
    master_key: Option<&EncryptionMasterKey>,
    resolved: &mut std::collections::HashMap<(String, String), DataKey>,
) -> crate::error::Result<()> {
    let granted_subjects: Vec<(String, String)> =
        crate::event_store::sensitive_field_subjects(sensitive_fields, payload)
            .into_iter()
            .filter(|(_, subject_value)| {
                crate::event_store::sensitive_field_is_granted(access_mapping, subject_value)
            })
            .collect();
    if granted_subjects.is_empty() {
        return Ok(());
    }
    let master_key = master_key.ok_or(encryption::Error::MasterKeyNotConfigured)?;
    for (subject_key, subject_value) in granted_subjects {
        if resolved.contains_key(&(subject_key.clone(), subject_value.clone())) {
            continue;
        }
        if let Some(data_key) = get_active_data_key(
            pool,
            bounded_context,
            &subject_key,
            &subject_value,
            master_key,
        )
        .await?
        {
            resolved.insert((subject_key, subject_value), data_key);
        }
    }
    Ok(())
}

/// The `id`s `insert_event_and_update_sync_projections`/`insert_command`
/// need for their own join-row writes, for exactly the `EncryptionKey`s a
/// `protect_sensitive_fields` call actually returned as used (`Event`/
/// `Command.encryption_keys`) - re-keyed back into `resolved` by
/// `(subject_key, subject_value)`, the only identity `EncryptionKey`
/// itself carries.
#[tracing::instrument(skip_all)]
pub fn encryption_key_ids(
    used: &[EncryptionKey],
    resolved: &std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
) -> Vec<i64> {
    used.iter()
        .map(|k| {
            resolved
                .get(&(k.subject_key.clone(), k.subject_value.clone()))
                .expect("every key protect_sensitive_fields used came from resolve_encryption_keys' own resolve_key closure, so it must still be in the map that built it")
                .1
        })
        .collect()
}

async fn get_active_encryption_key_row(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
) -> crate::error::Result<Option<EncryptionKeyRow>> {
    let schema = schema_ident(bounded_context);
    let row: Option<EncryptionKeyRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ENCRYPTION_KEY_COLUMNS} FROM {schema}.encryption_keys \
         WHERE subject_key = $1 AND subject_value = $2 AND status = 'active'"
    )))
    .bind(subject_key)
    .bind(subject_value)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// The active `EncryptionKey` for one subject, if any - `resolvers::
/// subject_erasure`'s own lookup before calling the pure `forget_subject`
/// (§SubjectErasure's `context key: EncryptionKey where ... status =
/// active`). No key material - `get_or_create_encryption_key` is the only
/// function that ever needs (and returns) that.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_active_encryption_key(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
) -> crate::error::Result<Option<EncryptionKey>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    Ok(
        get_active_encryption_key_row(pool, bounded_context, subject_key, subject_value)
            .await?
            .map(|row| row.to_domain(bc)),
    )
}

/// The real decrypt-on-read pass's own lookup - the active subject's
/// `DataKey`, unwrapped and ready to decrypt with, if any. Composes
/// `get_active_encryption_key_row` (already used by `get_or_create_encryption_key`)
/// with the identical `unwrap_row` that function already calls - no new
/// SQL. `None` covers both "never provisioned" and "destroyed by
/// `ForgetSubject`" identically, matching `resolve_data_keys_for_reading`'s
/// own doc comment for why that's correct crypto-shredding behaviour, not
/// a gap.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_active_data_key(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    master_key: &EncryptionMasterKey,
) -> crate::error::Result<Option<DataKey>> {
    match get_active_encryption_key_row(pool, bounded_context, subject_key, subject_value).await? {
        Some(row) => Ok(Some(unwrap_row(master_key, &row)?)),
        None => Ok(None),
    }
}

/// `projections::read_projection`'s own pre-resolution step - every active
/// `EncryptionKey`'s own `DataKey`, for `subject_value` alone, across
/// *every* `subject_key` namespace - unlike `get_active_data_key` above,
/// which needs the exact `subject_key` a declared `SensitiveField` names,
/// this is the automatic, undeclared case: a projection's own instance
/// `key` is a subject value with no known-in-advance namespace, so every
/// namespace with an active key for it is a real candidate (see
/// `encryption::decrypt_ciphertext_leaves`'s own doc comment for why
/// trying more than one candidate is safe, not a heuristic). Empty when
/// nothing matches - no `master_key` needed at all in that case, the
/// identical "nothing to resolve, no key required" property
/// `resolve_data_keys_for_reading` already has. A **non-empty** match
/// with `master_key: None` is the identical hard, actionable
/// configuration error `resolve_encryption_keys`/`resolve_data_keys_for_reading`
/// already raise for the same underlying reason - confirmed with the
/// user before building real decrypt-on-read at all.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_active_data_keys_for_subject_value(
    pool: &Pool,
    bounded_context: &str,
    subject_value: &str,
    master_key: Option<&EncryptionMasterKey>,
) -> crate::error::Result<Vec<DataKey>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<EncryptionKeyRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ENCRYPTION_KEY_COLUMNS} FROM {schema}.encryption_keys \
         WHERE subject_value = $1 AND status = 'active'"
    )))
    .bind(subject_value)
    .fetch_all(pool)
    .await?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let master_key = master_key.ok_or(encryption::Error::MasterKeyNotConfigured)?;
    rows.iter().map(|row| unwrap_row(master_key, row)).collect()
}

/// Persists `forget_subject`'s outcome - real crypto-shredding, not just
/// the status flip: `wrapped_key`/`wrap_nonce` are set to `NULL` in the
/// same statement, so even the master key can no longer recover this
/// subject's `DataKey` afterwards (see `forget_subject`'s own doc comment:
/// "this function's own caller enacts [it] by destroying the key row").
/// Scoped to `status = 'active'` in the `WHERE` clause purely
/// defensively, since the pure rule already rejects an inactive key
/// before this is ever called.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn destroy_encryption_key(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    destroyed_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.encryption_keys SET status = 'destroyed', destroyed_at = $1, \
         wrapped_key = NULL, wrap_nonce = NULL \
         WHERE subject_key = $2 AND subject_value = $3 AND status = 'active'"
    )))
    .bind(destroyed_at)
    .bind(subject_key)
    .bind(subject_value)
    .execute(pool)
    .await?;
    Ok(())
}

// --- Command ---

#[derive(sqlx::FromRow)]
struct CommandRow {
    external_id: String,
    command_type_name: String,
    payload: String,
    metadata_type: String,
    metadata_version: i64,
    metadata_client_id: String,
    metadata_created_at: DateTime<Utc>,
    // Codeberg issue #18 - `None` only for a row written before these
    // columns existed.
    metadata_correlation_id: Option<String>,
    metadata_causation_id: Option<String>,
    consistency_tags: Json<Vec<Tag>>,
    consistency_boundary: Option<i64>,
}

impl CommandRow {
    async fn into_domain(
        self,
        pool: &Pool,
        bounded_context: &str,
    ) -> crate::error::Result<Command> {
        let command_type =
            require_command_type(pool, bounded_context, &self.command_type_name).await?;
        Ok(Command {
            id: self.external_id,
            bounded_context: command_type.bounded_context.clone(),
            command_type,
            payload: self.payload,
            metadata: Metadata {
                r#type: self.metadata_type,
                version: self.metadata_version,
                client_id: self.metadata_client_id,
                created_at: self.metadata_created_at,
                correlation_id: self.metadata_correlation_id,
                causation_id: self.metadata_causation_id,
            },
            encryption_keys: Vec::new(),
            consistency_tags: self.consistency_tags.0,
            consistency_boundary: self.consistency_boundary,
        })
    }
}

// `external_id` - not `id`, this table's own internal `BIGSERIAL` primary
// key, purely an FK-linking detail `insert_command` returns separately
// and never surfaces on `Command` itself - is `Command.id` (drift audit
// finding #12): a real, `generate_token_id()`-assigned identity, added so
// `fetch_commands`'s own `triggered_event` lookup can match a specific
// command precisely rather than by whole-struct content equality.
const COMMAND_COLUMNS: &str = "external_id, command_type_name, payload, metadata_type, \
    metadata_version, metadata_client_id, metadata_created_at, metadata_correlation_id, \
    metadata_causation_id, consistency_tags, consistency_boundary";

/// Insert-only, unlike every `upsert_*` above - re-registration/promotion
/// don't apply to a `Command`, so every call is a new row, even though it
/// now has a real identity of its own (`Command.id`, drift audit finding
/// #12 - not this row's internal `BIGSERIAL id`, returned separately
/// below and never surfaced on the domain struct - see the migration's
/// own doc comment on `commands`). Returns the new row's internal `id`,
/// needed to link the `Event`s `process_command` produced back to it (see
/// `insert_event`'s own `command_id` parameter). `encryption_key_ids` is
/// `get_or_create_encryption_key`'s own returned `id`s for
/// `command.encryption_keys` - see `insert_event_and_update_sync_projections`'s
/// own doc comment on why the domain struct alone isn't enough to link
/// the join rows.
///
/// Takes an already-open `tx` rather than opening/committing its own
/// (previously the latter) - `ProcessCommand`'s own REST/GraphQL call
/// sites now share one outer transaction across `next_sequence`'s lock,
/// this insert, and every triggered `Event`'s own insert, so the command
/// row and every one of the events it triggered commit or roll back
/// together - see `DynamicConsistencyBoundaryHonoured` and the note above
/// the rules in specs/skilj.allium.
#[tracing::instrument(skip_all)]
pub async fn insert_command(
    tx: &mut Transaction<'_, Postgres>,
    command: &Command,
    encryption_key_ids: &[i64],
) -> crate::error::Result<i64> {
    let schema = schema_ident(&command.bounded_context.name);
    let (id,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.commands ({COMMAND_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id"
    )))
    .bind(&command.id)
    .bind(&command.command_type.name)
    .bind(&command.payload)
    .bind(&command.metadata.r#type)
    .bind(command.metadata.version)
    .bind(&command.metadata.client_id)
    .bind(command.metadata.created_at)
    .bind(&command.metadata.correlation_id)
    .bind(&command.metadata.causation_id)
    .bind(Json(&command.consistency_tags))
    .bind(command.consistency_boundary)
    .fetch_one(&mut **tx)
    .await?;

    // One multi-row insert via `unnest` instead of one round trip per
    // key - most commands carry zero or one, but a command whose
    // payload touches several `sensitive_field_subjects` can carry
    // several, and each used to be its own serialized `INSERT` inside
    // this transaction's own held lock (docs/architecture.md's load-test
    // §: `commit_command_batch`'s per-command critical section).
    if !encryption_key_ids.is_empty() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.command_encryption_keys (command_id, encryption_key_id) \
             SELECT $1, unnest($2::bigint[])"
        )))
        .bind(id)
        .bind(encryption_key_ids)
        .execute(&mut **tx)
        .await?;
    }

    Ok(id)
}

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_command_by_id(
    pool: &Pool,
    bounded_context: &str,
    id: i64,
) -> crate::error::Result<Option<Command>> {
    let schema = schema_ident(bounded_context);
    let row: Option<CommandRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_COLUMNS} FROM {schema}.commands WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(row) => Ok(Some(row.into_domain(pool, bounded_context).await?)),
        None => Ok(None),
    }
}

/// `get_command_by_id`'s batched sibling - one `WHERE id = ANY($1)`
/// round trip for however many distinct commands a caller needs, instead
/// of one round trip (each several deep via `CommandRow::into_domain`'s
/// own `get_command_type`) per id. Built for
/// `list_events_for_bounded_context_matching_tags_with_bc`'s own
/// row-processing loop (docs/architecture.md §61's round four) - a real
/// load test found resolving each `command_triggered` row's origin one
/// at a time was the single biggest cost inside `commit_command_batch`'s
/// own held lock, and a first, *concurrent* attempt at fixing it
/// measured worse (bursting many simultaneous connection requests
/// starved the next batch's own lock acquisition) before this batched
/// version replaced it. `ids` empty returns an empty map without
/// touching Postgres. Takes `bc` (not a bounded context name) for the
/// identical "caller already has this row, don't re-fetch it" reasoning
/// `get_event_type_with_bc` gives.
async fn get_commands_by_ids_with_bc(
    pool: &Pool,
    bc: &BoundedContext,
    ids: &[i64],
) -> crate::error::Result<std::collections::HashMap<i64, Command>> {
    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let schema = schema_ident(&bc.name);
    let rows: Vec<CommandRowWithId> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, {COMMAND_COLUMNS} FROM {schema}.commands WHERE id = ANY($1)"
    )))
    .bind(ids)
    .fetch_all(pool)
    .await?;

    let mut distinct_type_names: Vec<&str> = Vec::new();
    for row in &rows {
        if !distinct_type_names.contains(&row.command_type_name.as_str()) {
            distinct_type_names.push(&row.command_type_name);
        }
    }
    let command_types = get_command_types_by_names_with_bc(pool, bc, &distinct_type_names).await?;

    let mut commands = std::collections::HashMap::with_capacity(rows.len());
    for row in rows {
        // Unlike `get_command_type`'s own lookup (see `require_command_type`),
        // `get_command_types_by_names_with_bc` queries `{schema}.command_types`
        // directly with the `bc` already in hand - no internal existence
        // recheck to silently swallow. A `hard_delete_bounded_context` race
        // dropping the schema between the `commands` query above and this one
        // surfaces as a real `sqlx::Error` from that `.await?`, not a `None`
        // reaching this lookup - so a genuine miss here is a real bug in the
        // data (a `commands` row outliving the `command_types` row it names),
        // not a race, and this stays a panic.
        let command_type = command_types
            .get(&row.command_type_name)
            .expect("commands row references a command_types row that no longer exists")
            .clone();
        commands.insert(
            row.id,
            Command {
                id: row.external_id,
                bounded_context: command_type.bounded_context.clone(),
                command_type,
                payload: row.payload,
                metadata: Metadata {
                    r#type: row.metadata_type,
                    version: row.metadata_version,
                    client_id: row.metadata_client_id,
                    created_at: row.metadata_created_at,
                    correlation_id: row.metadata_correlation_id,
                    causation_id: row.metadata_causation_id,
                },
                encryption_keys: Vec::new(),
                consistency_tags: row.consistency_tags.0,
                consistency_boundary: row.consistency_boundary,
            },
        );
    }
    Ok(commands)
}

/// `CommandRow`'s own sibling with the internal `BIGSERIAL` id included -
/// `get_commands_by_ids_with_bc`'s own `WHERE id = ANY($1)` needs it to
/// map each returned row back to the id that requested it, which
/// `CommandRow` itself never carries (every other caller already knows
/// the id it asked for).
#[derive(sqlx::FromRow)]
struct CommandRowWithId {
    id: i64,
    external_id: String,
    command_type_name: String,
    payload: String,
    metadata_type: String,
    metadata_version: i64,
    metadata_client_id: String,
    metadata_created_at: DateTime<Utc>,
    metadata_correlation_id: Option<String>,
    metadata_causation_id: Option<String>,
    consistency_tags: Json<Vec<Tag>>,
    consistency_boundary: Option<i64>,
}

/// `get_command_type`'s batched sibling - `get_event_type_with_bc`'s own
/// precedent, extended to a whole set of names in one `WHERE name =
/// ANY($1)` round trip. `names` empty returns an empty map without
/// touching Postgres.
async fn get_command_types_by_names_with_bc(
    pool: &Pool,
    bc: &BoundedContext,
    names: &[&str],
) -> crate::error::Result<std::collections::HashMap<String, CommandType>> {
    if names.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let schema = schema_ident(&bc.name);
    let rows: Vec<CommandTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_TYPE_COLUMNS} FROM {schema}.command_types WHERE name = ANY($1)"
    )))
    .bind(names)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.name.clone(), row.into_domain(bc.clone())))
        .collect())
}

/// `get_command_by_id`'s own sibling, addressed by `Command.id`
/// (`commands.external_id`) instead of the internal `BIGSERIAL` - what a
/// caller holding a domain `Command.id` (the private-field mechanism's
/// own `grantPrivateFieldAccessForCommand` mutation, naming the command
/// being shared by its own wire id) needs, without knowing the internal
/// one at all.
pub async fn get_command_by_external_id(
    pool: &Pool,
    bounded_context: &str,
    external_id: &str,
) -> crate::error::Result<Option<Command>> {
    let schema = schema_ident(bounded_context);
    let row: Option<CommandRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_COLUMNS} FROM {schema}.commands WHERE external_id = $1"
    )))
    .bind(external_id)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(row) => Ok(Some(row.into_domain(pool, bounded_context).await?)),
        None => Ok(None),
    }
}

/// Every `Command` currently stored for a whole bounded context - the
/// full-snapshot parameter `fetch_commands`' own `bounded_context_commands`
/// expects (same treatment `list_events_for_bounded_context` gets for
/// `Event`), added propagating `skilj-graphql`'s `CommandQuery` resolver
/// (Phase 3).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_commands_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Command>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<CommandRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_COLUMNS} FROM {schema}.commands"
    )))
    .fetch_all(pool)
    .await?;

    let mut commands = Vec::with_capacity(rows.len());
    for row in rows {
        commands.push(row.into_domain(pool, bounded_context).await?);
    }
    Ok(commands)
}

// --- Projection / ProjectionRebuild ---

#[derive(sqlx::FromRow)]
struct ProjectionRow {
    name: String,
    schema: String,
    schema_version: i64,
    sync: bool,
    caught_up_to: Option<i64>,
}

/// Every `EventType` a `projection_consumed_event_types`-style join table
/// names for one projection - shared by `Projection.consumed_event_types`
/// and `ProjectionRebuild`'s own field of the same name, which read from
/// the two structurally identical join tables `join_table` names.
async fn consumed_event_types(
    pool: &Pool,
    bounded_context: &str,
    join_table: &str,
    projection_name: &str,
) -> crate::error::Result<Vec<EventType>> {
    let schema = schema_ident(bounded_context);
    let names: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name FROM {schema}.{join_table} WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .fetch_all(pool)
    .await?;

    let mut event_types = Vec::with_capacity(names.len());
    for (event_type_name,) in names {
        let et = get_event_type(pool, bounded_context, &event_type_name)
            .await?
            .expect(
                "consumed_event_types join row references an event_types row that no longer exists",
            );
        event_types.push(et);
    }
    Ok(event_types)
}

async fn replace_consumed_event_types(
    executor: &mut sqlx::PgConnection,
    bounded_context: &str,
    join_table: &str,
    projection_name: &str,
    event_types: &[EventType],
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.{join_table} WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .execute(&mut *executor)
    .await?;
    for et in event_types {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.{join_table} (projection_name, event_type_name) VALUES ($1,$2)"
        )))
        .bind(projection_name)
        .bind(&et.name)
        .execute(&mut *executor)
        .await?;
    }
    Ok(())
}

const PROJECTION_COLUMNS: &str = "name, schema, schema_version, sync, caught_up_to";

/// See `upsert_event_type` above - same upsert shape, plus replacing this
/// projection's `projection_consumed_event_types` join rows wholesale
/// (simpler and plenty fast enough for a small, admin-managed list than
/// diffing old vs. new membership).
///
/// Both statements run inside one transaction, not back-to-back on the
/// bare pool: two concurrent `upsert_projection` calls for the same
/// `projection.name` (e.g. a rolling deploy's two instances reconciling
/// at once, or - what actually surfaced this, see `skilj-demo`'s test
/// suite - two `#[test]`s racing to register the same projection) used
/// to each run their own `DELETE` then `INSERT` directly against `pool`
/// with no lock between them, so both could pass the `DELETE` before
/// either reached its `INSERT`, then both try to insert the identical
/// `(projection_name, event_type_name)` row and one loses to
/// `projection_consumed_event_types_pkey`. Wrapping the row upsert and
/// the join-table replace in a single transaction fixes this for free:
/// the `INSERT ... ON CONFLICT (name) DO UPDATE` against `projections`
/// takes (and, on the losing side, waits on) that row's lock for the
/// rest of the transaction, so a second concurrent call can't reach its
/// own `DELETE`/`INSERT` pair until the first has committed.
#[tracing::instrument(skip_all)]
pub async fn upsert_projection(pool: &Pool, projection: &Projection) -> crate::error::Result<()> {
    let schema = schema_ident(&projection.bounded_context.name);
    let mut tx = pool.begin().await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projections ({PROJECTION_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5) \
         ON CONFLICT (name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            sync = EXCLUDED.sync, caught_up_to = EXCLUDED.caught_up_to"
    )))
    .bind(&projection.name)
    .bind(&projection.schema)
    .bind(projection.schema_version)
    .bind(projection.sync)
    .bind(projection.caught_up_to)
    .execute(&mut *tx)
    .await?;
    replace_consumed_event_types(
        &mut tx,
        &projection.bounded_context.name,
        "projection_consumed_event_types",
        &projection.name,
        &projection.consumed_event_types,
    )
    .await?;
    tx.commit().await?;
    notify_registration_changed(pool).await;
    Ok(())
}

/// Get-or-create-with-lock for one projection instance's own
/// `projection_state` row, keyed by `(projection_name, key)` - the
/// insert-or-update `insert_event_and_update_sync_projections`/
/// `catch_up_bounded_context` both need now that instances are created
/// lazily, on first touch, rather than pre-seeded at registration
/// ([§9](../../../docs/architecture.md#next-steps)'s "keyed / multi-row Projections" pass). `ON CONFLICT ... DO
/// UPDATE SET state = {schema}.projection_state.state` is a no-op write
/// on the already-exists path - it exists purely so Postgres still
/// acquires the row lock there too (the identical guarantee a plain
/// `SELECT ... FOR UPDATE` gave the old always-pre-seeded schema),
/// without ever overwriting real accumulated state with
/// `default_state_json`. Generic over `impl sqlx::PgExecutor<'_>` (the
/// same generalisation `insert_event` itself already has) so both
/// callers can run this inside their own already-open transaction.
///
/// Also returns the row's own `as_of_sequence` (Codeberg issue #25's
/// investigation finding - see `apply_projection_fold_update`'s own doc
/// comment) - every caller needs it immediately after to decide whether
/// this specific row has already folded the event it's about to fold,
/// the same shape `get_or_create_snapshot_state_for_update` already
/// returns for `Snapshot`.
async fn get_or_create_projection_state_for_update(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    projection_name: &str,
    key: &str,
    default_state_json: &str,
) -> crate::error::Result<(i64, String)> {
    let (as_of_sequence, state): (i64, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_state (projection_name, key, state, updated_at) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (projection_name, key) DO UPDATE SET state = {schema}.projection_state.state \
         RETURNING as_of_sequence, state"
    )))
    .bind(projection_name)
    .bind(key)
    .bind(default_state_json)
    .fetch_one(executor)
    .await?;
    Ok((as_of_sequence, state))
}

/// `get_or_create_projection_state_for_update`'s own twin for
/// `projection_rebuild_state` - see that function's own doc comment for
/// the "no-op write, purely to acquire the lock" reasoning and the
/// returned `as_of_sequence`, both identical here. `status` is always
/// `'building'` inline, not a parameter - a pending row is never folded
/// (only `catch_up_bounded_context`'s own `building_rebuilds` walk
/// reaches this function at all), so there is no other status any real
/// caller could mean.
async fn get_or_create_projection_rebuild_state_for_update(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    projection_name: &str,
    key: &str,
    default_state_json: &str,
) -> crate::error::Result<(i64, String)> {
    let (as_of_sequence, state): (i64, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_rebuild_state (projection_name, status, key, state, \
         updated_at) VALUES ($1, 'building', $2, $3, now()) \
         ON CONFLICT (projection_name, status, key) DO UPDATE SET \
         state = {schema}.projection_rebuild_state.state \
         RETURNING as_of_sequence, state"
    )))
    .bind(projection_name)
    .bind(key)
    .bind(default_state_json)
    .fetch_one(executor)
    .await?;
    Ok((as_of_sequence, state))
}

/// Applies one projection fold's `UPDATE ... SET state = ...` - shared by
/// every "fold one event, persist the new state" call site
/// (`insert_event_and_update_sync_projections_in_tx`, both of
/// `catch_up_bounded_context`'s live and rebuild-building loops, and
/// `fold_history_into_new_sync_projection`), which were four near-identical
/// copies of the same statement before this pass. `table` is
/// `"projection_state"` or `"projection_rebuild_state"`; `extra_where` is
/// appended to the `WHERE` clause verbatim - `""` for the live table,
/// `" AND status = 'building'"` for the rebuild one, the same
/// distinction `get_or_create_projection_rebuild_state_for_update`'s own
/// hard-coded `'building'` already draws.
///
/// Also derives and persists this instance's own `owner` column
/// alongside `state` - cross-tenant projection read fix
/// (docs/architecture.md's own write-up of this pass). When
/// `owner_tag_key` is `Some` and `event.tags` carries a tag with that
/// key *and* a non-null value, that value becomes this row's own
/// `owner`; otherwise `owner` is left exactly as already stored - an
/// event lacking the tag (or carrying it with a null value, the "mapped
/// field was absent" case - see `Tag.value` in the spec) never clears an
/// already-established owner. See `plugin::Projection::OWNER_TAG_KEY`'s
/// own doc comment for the full contract.
///
/// Also advances `as_of_sequence` to `event.sequence` (Codeberg issue
/// #25's investigation finding) - every call site's own `event.sequence`
/// only ever increases within its own walk, so a plain unconditional
/// `SET` is correct here without needing a `GREATEST(...)` guard; what
/// actually makes this safe under two instances racing the same row is
/// each call site's own new pre-fold check against the value
/// `get_or_create_projection_state_for_update`/`..._rebuild_state_for_update`
/// just returned, *before* `dispatcher.project()` is ever called - by the
/// time this function runs, the caller has already decided this event
/// genuinely hasn't been folded into this row yet.
#[allow(clippy::too_many_arguments)]
async fn apply_projection_fold_update(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    table: &str,
    extra_where: &str,
    projection_name: &str,
    key: &str,
    new_state: &str,
    owner_tag_key: Option<&str>,
    event: &Event,
) -> crate::error::Result<()> {
    let owner = owner_tag_key.and_then(|owner_tag_key| {
        event
            .tags
            .iter()
            .find(|tag| tag.key == owner_tag_key)
            .and_then(|tag| tag.value.clone())
    });
    match owner {
        Some(owner) => {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.{table} SET state = $1, owner = $2, as_of_sequence = $3, \
                 updated_at = now() WHERE projection_name = $4 AND key = $5{extra_where}"
            )))
            .bind(new_state)
            .bind(owner)
            .bind(event.sequence)
            .bind(projection_name)
            .bind(key)
            .execute(executor)
            .await?;
        }
        None => {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.{table} SET state = $1, as_of_sequence = $2, updated_at = now() \
                 WHERE projection_name = $3 AND key = $4{extra_where}"
            )))
            .bind(new_state)
            .bind(event.sequence)
            .bind(projection_name)
            .bind(key)
            .execute(executor)
            .await?;
        }
    }
    Ok(())
}

/// [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 2" - get-or-create-with-lock for
/// one snapshot's own `(snapshot_name, tag_key, tag_value)` row, the
/// `Snapshot` counterpart to `get_or_create_projection_state_for_update`
/// above. `version` is the currently-registered `Snapshot::VERSION` -
/// on a fresh row, seeds it at `-1`/`default_state_json`; on an existing
/// row whose own stored `snapshot_version` still matches, this is a
/// no-op write purely to acquire the lock (identical reasoning to the
/// `Projection` twin); on an existing row at an *older* version, resets
/// it to `-1`/`default_state_json` at the new version, atomically as
/// part of acquiring the lock - the "model changed" case
/// `CommandType::decide_from_snapshot`'s own doc comment describes,
/// done here rather than as a separate read-then-write (which could
/// race two concurrent catch-up ticks against the same row).
async fn get_or_create_snapshot_state_for_update(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    snapshot_name: &str,
    tag_key: &str,
    tag_value: &str,
    version: u64,
    default_state_json: &str,
) -> crate::error::Result<(i64, String)> {
    let (as_of_sequence, state): (i64, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.snapshots \
            (snapshot_name, tag_key, tag_value, snapshot_version, as_of_sequence, state, updated_at) \
         VALUES ($1, $2, $3, $4, -1, $5::jsonb, now()) \
         ON CONFLICT (snapshot_name, tag_key, tag_value) DO UPDATE SET \
            snapshot_version = CASE WHEN {schema}.snapshots.snapshot_version = $4 \
                THEN {schema}.snapshots.snapshot_version ELSE $4 END, \
            as_of_sequence = CASE WHEN {schema}.snapshots.snapshot_version = $4 \
                THEN {schema}.snapshots.as_of_sequence ELSE -1 END, \
            state = CASE WHEN {schema}.snapshots.snapshot_version = $4 \
                THEN {schema}.snapshots.state ELSE $5::jsonb END \
         RETURNING as_of_sequence, state::text"
    )))
    .bind(snapshot_name)
    .bind(tag_key)
    .bind(tag_value)
    .bind(version as i64)
    .bind(default_state_json)
    .fetch_one(executor)
    .await?;
    Ok((as_of_sequence, state))
}

/// `(snapshot_version, as_of_sequence, state, updated_at)` -
/// `get_snapshot_state`'s own return shape, named to satisfy
/// `clippy::type_complexity` rather than because anything else reuses
/// it.
type SnapshotStateRow = (u64, i64, String, DateTime<Utc>);

/// One snapshot's own materialised state, JSON-encoded, alongside the
/// `snapshot_version` it was written at, the `sequence` it's folded up
/// to, and when that last happened - `None` when nothing has ever been
/// written for this `(snapshot_name, tag_key, tag_value)` triple. Every
/// caller (the GraphQL inspection endpoint, and `submit_command`'s own
/// snapshot-accelerated `decide_from_snapshot` path) treats both
/// "nothing yet" and "a stored `snapshot_version` that no longer
/// matches the currently-registered `Snapshot::VERSION`" identically -
/// a cold start, not an error - see `CommandType::decide_from_snapshot`'s
/// own doc comment.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_snapshot_state(
    pool: &Pool,
    bounded_context: &str,
    snapshot_name: &str,
    tag_key: &str,
    tag_value: &str,
) -> crate::error::Result<Option<SnapshotStateRow>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(i64, i64, String, DateTime<Utc>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT snapshot_version, as_of_sequence, state::text, updated_at FROM {schema}.snapshots \
         WHERE snapshot_name = $1 AND tag_key = $2 AND tag_value = $3"
    )))
        .bind(snapshot_name)
        .bind(tag_key)
        .bind(tag_value)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(version, as_of_sequence, state, updated_at)| {
        (version as u64, as_of_sequence, state, updated_at)
    }))
}

/// `get_snapshot_state`'s own sibling, also returning the row's `owner`
/// column - cross-tenant read fix (docs/architecture.md's own write-up
/// of these passes). A separate function rather than changing
/// `get_snapshot_state`'s own return shape: that function's other
/// caller, `submit_command`'s snapshot-accelerated `decide_from_snapshot`
/// path, is a write-path internal accelerator, not a caller read -
/// deliberately untouched here, the same "`ProcessCommand`'s
/// `matching_events` is deliberately untouched" principle the raw-event
/// pass already stated. Only `snapshot_query::inspect_snapshot_field`,
/// which needs `owner` to enforce `RoleAccessMapping.scope`, calls this
/// one instead.
pub async fn get_snapshot_state_and_owner(
    pool: &Pool,
    bounded_context: &str,
    snapshot_name: &str,
    tag_key: &str,
    tag_value: &str,
) -> crate::error::Result<Option<(SnapshotStateRow, Option<String>)>> {
    type RawSnapshotStateAndOwnerRow = (i64, i64, String, DateTime<Utc>, Option<String>);
    let schema = schema_ident(bounded_context);
    let row: Option<RawSnapshotStateAndOwnerRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT snapshot_version, as_of_sequence, state::text, updated_at, owner \
         FROM {schema}.snapshots WHERE snapshot_name = $1 AND tag_key = $2 AND tag_value = $3"
    )))
    .bind(snapshot_name)
    .bind(tag_key)
    .bind(tag_value)
    .fetch_optional(pool)
    .await?;
    Ok(
        row.map(|(version, as_of_sequence, state, updated_at, owner)| {
            ((version as u64, as_of_sequence, state, updated_at), owner)
        }),
    )
}

/// What `submit_command`'s own snapshot-accelerated `decide_from_snapshot`
/// call needs, resolved from a stored `snapshots` row (or a cold start).
pub struct ResolvedSnapshot {
    pub state_json: String,
    pub as_of_sequence: i64,
}

/// Shared by `skilj-graphql`'s `submitCommand` resolver and `skilj-rest`'s
/// `post_commands_trigger` route ([docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)), so the "does
/// this command take the snapshot-accelerated path" decision lives once,
/// not duplicated per surface the way plenty of resolver-local logic
/// legitimately is elsewhere - this one has real, non-trivial rules
/// worth keeping in exactly one place.
///
/// `None` when `snapshot_name` isn't actually registered
/// (`SnapshotDispatcher::tag_key` returning `None`), or when
/// `consistency_tags` isn't *exactly* the snapshot's own single
/// `TAG_KEY` - `CommandType::snapshot()`'s own doc comment on why a
/// multi-tag command opting in is a silent fallback, not an error.
/// `Some` otherwise: a fresh/cold snapshot (nothing stored yet, or a
/// stored `snapshot_version` that no longer matches the currently-
/// registered one - "model changed", `CommandType::decide_from_snapshot`'s
/// own doc comment) resolves to `Snapshot::State::default()`,
/// JSON-encoded, at `as_of_sequence: -1` - the same cold-start shape
/// `get_or_create_snapshot_state_for_update` gives the background
/// catch-up task.
pub async fn resolve_snapshot_context(
    pool: &Pool,
    bounded_context: &str,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    snapshot_name: &str,
    consistency_tags: &[Tag],
) -> crate::error::Result<Option<ResolvedSnapshot>> {
    let Some(tag_key) = snapshot_dispatcher.tag_key(bounded_context, snapshot_name) else {
        return Ok(None);
    };
    let [only_tag] = consistency_tags else {
        return Ok(None);
    };
    if only_tag.key != tag_key {
        return Ok(None);
    }
    let Some(tag_value) = &only_tag.value else {
        return Ok(None);
    };

    let version = snapshot_dispatcher
        .version(bounded_context, snapshot_name)
        .unwrap_or(0);
    let stored =
        get_snapshot_state(pool, bounded_context, snapshot_name, tag_key, tag_value).await?;
    let (as_of_sequence, state_json) = match stored {
        Some((stored_version, as_of_sequence, state_json, _updated_at))
            if stored_version == version =>
        {
            (as_of_sequence, state_json)
        }
        _ => (
            -1,
            snapshot_dispatcher
                .default_state(bounded_context, snapshot_name)
                .unwrap_or_default(),
        ),
    };
    Ok(Some(ResolvedSnapshot {
        state_json,
        as_of_sequence,
    }))
}

/// The background catch-up task's own progress marker for every
/// registered snapshot in one bounded context, in one query - Codeberg
/// issue #15's batched replacement for what used to be a `get_snapshot_progress`
/// call per snapshot name (one query each). A name absent from the
/// returned map has never had a catch-up tick yet - `catch_up_snapshots`
/// treats that the same as this function's own predecessor did
/// (`unwrap_or(-1)` at the call site), just without a per-name round
/// trip to discover it. Plain read, no lock: only used to compute
/// `catch_up_snapshots`' own starting point for the *next* tick, never
/// to decide whether a specific write is safe - that's
/// `get_or_create_snapshot_state_for_update`'s own job, per row.
async fn list_snapshot_progress_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<std::collections::HashMap<String, i64>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT snapshot_name, caught_up_to FROM {schema}.snapshot_progress"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// Advances (or seeds) one snapshot's own progress marker - called once
/// per event `catch_up_snapshots` processes, for every registered
/// snapshot, regardless of whether that particular event actually
/// touched any of its rows - the identical "position always advances,
/// state only changes when consumed" treatment `Projection`'s own
/// `caught_up_to` already gets.
async fn upsert_snapshot_progress(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    snapshot_name: &str,
    caught_up_to: i64,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.snapshot_progress (snapshot_name, caught_up_to) VALUES ($1, $2) \
         ON CONFLICT (snapshot_name) DO UPDATE SET caught_up_to = $2"
    )))
    .bind(snapshot_name)
    .bind(caught_up_to)
    .execute(executor)
    .await?;
    Ok(())
}

/// One projection instance's own materialised state, JSON-encoded -
/// `None` when nothing has touched this `(projection_name, key)` pair
/// yet (a legitimate, common state now that instances are created
/// lazily - a customer with no purchase history yet, say - not an
/// error; `resolvers::projection_query` falls back to
/// `ProjectionDispatcher::default_state` for it, the same value a fresh
/// instance would lazily start from).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_projection_state(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    key: &str,
) -> crate::error::Result<Option<String>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT state FROM {schema}.projection_state WHERE projection_name = $1 AND key = $2"
    )))
    .bind(projection_name)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(state,)| state))
}

/// `get_projection_state`'s own twin, also returning the row's `owner`
/// column - cross-tenant projection read fix (docs/architecture.md's own
/// write-up of this pass). A separate function rather than changing
/// `get_projection_state`'s own return shape: that function has many
/// existing callers (tests included) uninterested in `owner` at all: only
/// `projection_query`'s resolver, which needs `owner` to enforce
/// `RoleAccessMapping.scope` (see `projections::query_projection`), calls
/// this one instead. `None` for "no row at all" (an untouched key) is not
/// distinguished from `Some((state, None))` at the SQL level by this
/// function - only the caller decides what "no proven owner" versus "no
/// row yet" means for its own enforcement.
pub async fn get_projection_state_and_owner(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    key: &str,
) -> crate::error::Result<Option<(String, Option<String>)>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(String, Option<String>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT state, owner FROM {schema}.projection_state \
         WHERE projection_name = $1 AND key = $2"
    )))
    .bind(projection_name)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// A building `ProjectionRebuild`'s own materialised state, JSON-encoded,
/// for one instance/`key`. `None` when either nothing is staged at all
/// or a staged rebuild hasn't reached this key yet (`catch_up_bounded_context`
/// hasn't reached it in a poll tick yet, or the dispatcher has never
/// recognised it - see `ProjectionDispatcher::default_state`). Mirrors
/// `get_projection_state` above - for tests and anything else wanting to
/// read this row directly outside the consumer's own row-locked
/// transaction.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_projection_rebuild_state(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    key: &str,
) -> crate::error::Result<Option<String>> {
    let schema = schema_ident(bounded_context);
    // Always the `'building'` row - see `get_or_create_projection_rebuild_state_for_update`'s
    // own doc comment for why no other status is meaningful here.
    let row: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT state FROM {schema}.projection_rebuild_state \
         WHERE projection_name = $1 AND status = 'building' AND key = $2"
    )))
    .bind(projection_name)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(state,)| state))
}

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_projection(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<Projection>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let Some(row): Option<ProjectionRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_COLUMNS} FROM {schema}.projections WHERE name = $1"
    )))
    .bind(name)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    let consumed = consumed_event_types(
        pool,
        bounded_context,
        "projection_consumed_event_types",
        name,
    )
    .await?;
    Ok(Some(Projection {
        bounded_context: bc,
        name: row.name,
        schema: row.schema,
        schema_version: row.schema_version,
        consumed_event_types: consumed,
        sync: row.sync,
        caught_up_to: row.caught_up_to,
    }))
}

/// Every `Projection` registered in one bounded context - backs
/// `TypeRegistration`'s own `exposes: for projection in
/// bounded_context.projections`. Same per-row `consumed_event_types`
/// resolution `get_projection` does, just for every row in the context's
/// own `projections` table rather than one name.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_projections_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Projection>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<ProjectionRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_COLUMNS} FROM {schema}.projections"
    )))
    .fetch_all(pool)
    .await?;

    let mut projections = Vec::with_capacity(rows.len());
    for row in rows {
        let consumed = consumed_event_types(
            pool,
            bounded_context,
            "projection_consumed_event_types",
            &row.name,
        )
        .await?;
        projections.push(Projection {
            bounded_context: bc.clone(),
            name: row.name,
            schema: row.schema,
            schema_version: row.schema_version,
            consumed_event_types: consumed,
            sync: row.sync,
            caught_up_to: row.caught_up_to,
        });
    }
    Ok(projections)
}

/// The `sync`-only slice of `list_projections_for_bounded_context` that
/// `insert_event_and_update_sync_projections_in_tx` actually needs -
/// pulled out so every call site fetches it once, itself, before opening
/// (or as part of preparing) its own transaction, instead of that
/// function re-running the full metadata read once per event it inserts.
/// A command that decides several events used to pay for this read again
/// for every one of them, all of it while `submit_command`'s own
/// bounded-context lock was held (Codeberg issue #32) - hoisting it here,
/// to one call per commit, is pure round-trip reduction, not a behaviour
/// change: it is still the same "small, admin-managed list, not worth
/// locking" read via `pool`, not `tx`, that function's own doc comment
/// already describes.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn sync_projections_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Projection>> {
    Ok(list_projections_for_bounded_context(pool, bounded_context)
        .await?
        .into_iter()
        .filter(|p| p.sync)
        .collect())
}

#[derive(sqlx::FromRow)]
struct ProjectionRebuildRow {
    projection_name: String,
    schema: String,
    schema_version: i64,
    sync: bool,
    caught_up_to: Option<i64>,
    status: String,
}

fn projection_rebuild_status_to_str(status: ProjectionRebuildStatus) -> &'static str {
    match status {
        ProjectionRebuildStatus::Pending => "pending",
        ProjectionRebuildStatus::Building => "building",
    }
}

fn projection_rebuild_status_from_str(s: &str) -> ProjectionRebuildStatus {
    match s {
        "building" => ProjectionRebuildStatus::Building,
        _ => ProjectionRebuildStatus::Pending,
    }
}

const PROJECTION_REBUILD_COLUMNS: &str =
    "projection_name, schema, schema_version, sync, caught_up_to, status";

/// `consumed_event_types`/`replace_consumed_event_types` above's own twin
/// for `projection_rebuild_consumed_event_types` - not built on those
/// generic helpers directly, since that table now carries a `status`
/// column the plain `projections`/`projection_consumed_event_types` pair
/// never needed (see `provision_bounded_context_schema`'s own doc comment
/// on why: a pending row and a building row each need their own
/// consumed-types set, never mixed up).
async fn rebuild_consumed_event_types(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    status: ProjectionRebuildStatus,
) -> crate::error::Result<Vec<EventType>> {
    let schema = schema_ident(bounded_context);
    let names: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(projection_rebuild_status_to_str(status))
    .fetch_all(pool)
    .await?;

    let mut event_types = Vec::with_capacity(names.len());
    for (event_type_name,) in names {
        let et = get_event_type(pool, bounded_context, &event_type_name)
            .await?
            .expect(
                "projection_rebuild_consumed_event_types join row references an event_types \
                 row that no longer exists",
            );
        event_types.push(et);
    }
    Ok(event_types)
}

async fn replace_rebuild_consumed_event_types(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    status: ProjectionRebuildStatus,
    event_types: &[EventType],
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let status = projection_rebuild_status_to_str(status);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(status)
    .execute(pool)
    .await?;
    for et in event_types {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.projection_rebuild_consumed_event_types \
             (projection_name, status, event_type_name) VALUES ($1,$2,$3)"
        )))
        .bind(projection_name)
        .bind(status)
        .bind(&et.name)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Upsert **within `rebuild.status`'s own row** - matching
/// `RegisterProjection`'s own "restaging replaces the pending row"
/// semantics (`ON CONFLICT (projection_name, status)`, not
/// `projection_name` alone - see `provision_bounded_context_schema`'s own
/// doc comment on why one projection can have both a pending and a
/// building row at once). Never call this to persist a *status
/// transition* (pending becoming building) - that would leave the old
/// row behind as a stale duplicate under its own now-wrong status instead
/// of replacing it; see `transition_projection_rebuild_to_building` for
/// that case, the only one `rebuild_projection`'s own output is ever fed
/// into.
///
/// Note that `register_projection`'s own struct-update construction
/// (`ProjectionRebuild { caught_up_to: None, ..staged.clone() }`) sets
/// `caught_up_to` back to `None` on every restage - any progress
/// `catch_up_bounded_context` had already made toward the old
/// `schema`/`consumed_event_types` is invalidated the moment this is
/// called. This function doesn't touch `projection_rebuild_state` itself,
/// since `catch_up_bounded_context` is what notices `caught_up_to = None`
/// and resets that row, using whatever the *current* dispatcher considers
/// the default (see `ProjectionDispatcher::default_state`'s own doc
/// comment for why it, not this function, has to be the one deciding
/// that value).
#[tracing::instrument(skip_all)]
pub async fn upsert_projection_rebuild(
    pool: &Pool,
    rebuild: &ProjectionRebuild,
) -> crate::error::Result<()> {
    let schema = schema_ident(&rebuild.projection.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_rebuilds ({PROJECTION_REBUILD_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6) \
         ON CONFLICT (projection_name, status) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            sync = EXCLUDED.sync, caught_up_to = EXCLUDED.caught_up_to"
    )))
    .bind(&rebuild.projection.name)
    .bind(&rebuild.schema)
    .bind(rebuild.schema_version)
    .bind(rebuild.sync)
    .bind(rebuild.caught_up_to)
    .bind(projection_rebuild_status_to_str(rebuild.status))
    .execute(pool)
    .await?;
    replace_rebuild_consumed_event_types(
        pool,
        &rebuild.projection.bounded_context.name,
        &rebuild.projection.name,
        rebuild.status,
        &rebuild.consumed_event_types,
    )
    .await
}

/// Persists `rebuild_projection`'s own output - a pending row transitioning
/// to building, per `rule RebuildProjection`'s `ensures: staged.status =
/// building`. One transaction: the old `(projection_name, 'pending')` row
/// and its own join rows are deleted, and the new `(projection_name,
/// 'building')` row (`rebuild`, already carrying `status: Building`) is
/// inserted in their place - never both left behind as if this were two
/// independent rows, and never a window in which neither exists if this
/// fails partway. `ON CONFLICT (projection_name, status) DO UPDATE` on the
/// insert half is defensive, not the expected path: `RebuildProjection`'s
/// own `requires: staged.status = pending` doesn't check for an
/// *already*-building row, so a caller triggering it while one is still
/// replaying re-stages the build under the freshly toggled row rather
/// than erroring - still at most one building row afterward, satisfying
/// `UniqueRebuildPerProjectionAndStatus` either way.
#[tracing::instrument(skip_all)]
pub async fn transition_projection_rebuild_to_building(
    pool: &Pool,
    rebuild: &ProjectionRebuild,
) -> crate::error::Result<()> {
    debug_assert_eq!(
        rebuild.status,
        ProjectionRebuildStatus::Building,
        "transition_projection_rebuild_to_building's own rebuild must already carry status: \
         Building - rebuild_projection's own output does"
    );
    let mut tx = pool.begin().await?;
    let schema = schema_ident(&rebuild.projection.bounded_context.name);
    let pending = projection_rebuild_status_to_str(ProjectionRebuildStatus::Pending);

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(&rebuild.projection.name)
    .bind(pending)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuilds WHERE projection_name = $1 AND status = $2"
    )))
    .bind(&rebuild.projection.name)
    .bind(pending)
    .execute(&mut *tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_rebuilds ({PROJECTION_REBUILD_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6) \
         ON CONFLICT (projection_name, status) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            sync = EXCLUDED.sync, caught_up_to = EXCLUDED.caught_up_to"
    )))
    .bind(&rebuild.projection.name)
    .bind(&rebuild.schema)
    .bind(rebuild.schema_version)
    .bind(rebuild.sync)
    .bind(rebuild.caught_up_to)
    .bind(projection_rebuild_status_to_str(rebuild.status))
    .execute(&mut *tx)
    .await?;
    let building = projection_rebuild_status_to_str(rebuild.status);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(&rebuild.projection.name)
    .bind(building)
    .execute(&mut *tx)
    .await?;
    for et in &rebuild.consumed_event_types {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.projection_rebuild_consumed_event_types \
             (projection_name, status, event_type_name) VALUES ($1,$2,$3)"
        )))
        .bind(&rebuild.projection.name)
        .bind(building)
        .bind(&et.name)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// `None` when this projection has no rebuild staged *at this `status`* -
/// the `staged` parameter `register_projection`/`rebuild_projection`/
/// `discard_projection_rebuild` each expect "as already looked up by the
/// caller" (see their own doc comments) is always the *pending* row per
/// the spec's own `let staged = ProjectionRebuild{projection: ...,
/// status: pending}` in every one of those three rules -
/// `catch_up_bounded_context` is the one caller that asks for `Building`
/// instead.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_projection_rebuild(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    status: ProjectionRebuildStatus,
) -> crate::error::Result<Option<ProjectionRebuild>> {
    let Some(projection) = get_projection(pool, bounded_context, projection_name).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let Some(row): Option<ProjectionRebuildRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_REBUILD_COLUMNS} FROM {schema}.projection_rebuilds \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(projection_rebuild_status_to_str(status))
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    debug_assert_eq!(
        row.projection_name, projection_name,
        "projection_rebuilds row matched the WHERE clause but its own projection_name column disagrees"
    );
    let consumed =
        rebuild_consumed_event_types(pool, bounded_context, projection_name, status).await?;
    Ok(Some(ProjectionRebuild {
        projection,
        schema: row.schema,
        schema_version: row.schema_version,
        consumed_event_types: consumed,
        sync: row.sync,
        caught_up_to: row.caught_up_to,
        status: projection_rebuild_status_from_str(&row.status),
    }))
}

/// Persists `discard_projection_rebuild`'s outcome - the spec's own
/// `ensures` is a deletion (`not exists staged`), which that pure
/// function can't perform itself (see its own doc comment); this is the
/// caller-side deletion it hands back to. `status` is always `Pending` in
/// practice - `DiscardProjectionRebuild`'s own `staged` is a pending row
/// by definition (see `rule DiscardProjectionRebuild`'s `let`) - but a
/// real parameter rather than hardcoded, matching `get_projection_rebuild`'s
/// own shape, since the caller (not this function) is what already knows
/// which row `discard_projection_rebuild`'s own `staged` argument was.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn delete_projection_rebuild(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    status: ProjectionRebuildStatus,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let status = projection_rebuild_status_to_str(status);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(status)
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuilds WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}

// --- RoleAccessMapping ---

#[derive(sqlx::FromRow)]
struct RoleAccessMappingRow {
    role_id: String,
    bounded_context: String,
    level: String,
    can_read_sensitive: bool,
    scope: Option<String>,
    status: String,
    created_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
}

impl RoleAccessMappingRow {
    /// Needs `pool` (unlike every other row type's `into_domain`) since
    /// `RoleAccessMapping` embeds a whole `Role` and a whole
    /// `BoundedContext`, not just their ids - two more queries per row,
    /// the same N+1 trade-off `get_event_type`'s own `BoundedContext`
    /// lookup already makes for a simpler implementation over a joined
    /// query. Worth revisiting if this ever shows up in a profile; a
    /// small admin-managed table is an unlikely place for that to matter.
    ///
    /// `Ok(None)`, not a panic, when the role or bounded context this
    /// row points to is gone by the time the follow-up query runs:
    /// `hard_delete_bounded_context`'s `DROP SCHEMA`+`DELETE` and its
    /// `ON DELETE CASCADE` onto this very table mean no *committed*
    /// state ever has a `role_access_mappings` row outliving its
    /// `bounded_contexts` row - but this method's own initial `SELECT`
    /// and this follow-up lookup are two separate, unsynchronized
    /// queries, not one snapshot, so a concurrent hard delete landing
    /// in that gap is exactly this: a row this method legitimately read
    /// a moment ago, now legitimately gone. Treating that as "wasn't in
    /// the snapshot after all" rather than panicking is the same call
    /// `list_role_access_mappings`'s "gone by the time you look" case
    /// deserves anywhere it's read without a shared transaction.
    async fn into_domain(self, pool: &Pool) -> crate::error::Result<Option<RoleAccessMapping>> {
        let Some(role) = get_role(pool, &self.role_id).await? else {
            return Ok(None);
        };
        let Some(bounded_context) = get_bounded_context(pool, &self.bounded_context).await? else {
            return Ok(None);
        };
        Ok(Some(RoleAccessMapping {
            role,
            bounded_context,
            level: access_level_from_str(&self.level),
            can_read_sensitive: self.can_read_sensitive,
            scope: self.scope,
            status: role_status_from_str(&self.status),
            created_at: self.created_at,
            revoked_at: self.revoked_at,
        }))
    }
}

const ROLE_ACCESS_MAPPING_COLUMNS: &str =
    "role_id, bounded_context, level, can_read_sensitive, scope, status, created_at, revoked_at";

#[tracing::instrument(skip_all)]
pub async fn insert_role_access_mapping(
    pool: &Pool,
    mapping: &RoleAccessMapping,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO role_access_mappings ({ROLE_ACCESS_MAPPING_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
    )))
    .bind(&mapping.role.id)
    .bind(&mapping.bounded_context.name)
    .bind(access_level_to_str(mapping.level))
    .bind(mapping.can_read_sensitive)
    .bind(&mapping.scope)
    .bind(role_status_to_str(mapping.status))
    .bind(mapping.created_at)
    .bind(mapping.revoked_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// `None` when this `(role, bounded_context)` pair has no *active*
/// mapping - a revoked one (if any) doesn't count, matching every rule
/// in `access_control` that reads "the" mapping for a pair
/// (`grant_role_access_mapping`'s `DuplicateActiveMapping` check,
/// `revoke_role_access_mapping`'s own lookup). The partial unique index
/// on `(role_id, bounded_context) WHERE status = 'active'` guarantees at
/// most one row can ever match.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_active_role_access_mapping(
    pool: &Pool,
    role_id: &str,
    bounded_context: &str,
) -> crate::error::Result<Option<RoleAccessMapping>> {
    let row: Option<RoleAccessMappingRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ROLE_ACCESS_MAPPING_COLUMNS} FROM role_access_mappings \
         WHERE role_id = $1 AND bounded_context = $2 AND status = 'active'"
    )))
    .bind(role_id)
    .bind(bounded_context)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(row) => row.into_domain(pool).await,
        None => Ok(None),
    }
}

/// Every `RoleAccessMapping` this engine currently knows of, active or
/// not - the full-snapshot parameter `grant_role_access_mapping`'s own
/// `existing_mappings` expects (see its doc comment). Same "small,
/// admin-managed, unscoped is fine" reasoning as `list_roles`. A row
/// whose role or bounded context was concurrently hard-deleted between
/// this method's own `SELECT` and `RoleAccessMappingRow::into_domain`'s
/// follow-up lookups is silently dropped, not an error - see that
/// method's own doc comment.
#[tracing::instrument(skip_all)]
pub async fn list_role_access_mappings(
    pool: &Pool,
) -> crate::error::Result<Vec<RoleAccessMapping>> {
    let rows: Vec<RoleAccessMappingRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ROLE_ACCESS_MAPPING_COLUMNS} FROM role_access_mappings"
    )))
    .fetch_all(pool)
    .await?;
    let mut mappings = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(mapping) = row.into_domain(pool).await? {
            mappings.push(mapping);
        }
    }
    Ok(mappings)
}

/// Every currently-*active* `RoleAccessMapping` for one `Role` - the
/// `active_mappings` parameter `revoke_role` expects (see its own doc
/// comment: "as already looked up by the caller").
#[tracing::instrument(skip_all)]
pub async fn list_active_role_access_mappings_for_role(
    pool: &Pool,
    role_id: &str,
) -> crate::error::Result<Vec<RoleAccessMapping>> {
    let rows: Vec<RoleAccessMappingRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ROLE_ACCESS_MAPPING_COLUMNS} FROM role_access_mappings \
         WHERE role_id = $1 AND status = 'active'"
    )))
    .bind(role_id)
    .fetch_all(pool)
    .await?;
    let mut mappings = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(mapping) = row.into_domain(pool).await? {
            mappings.push(mapping);
        }
    }
    Ok(mappings)
}

/// Persists a `revoke_role_access_mapping` (or `revoke_role`'s own
/// cascade) outcome directly, by the same unambiguous `(role_id,
/// bounded_context, status = 'active')` triple `get_active_role_access_mapping`
/// reads by - safe without needing a synthetic id, since at most one row
/// can ever match (see the migration's own partial unique index). `impl
/// PgExecutor` - same reasoning as `update_role`'s own doc comment.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn revoke_active_role_access_mapping<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    role_id: &str,
    bounded_context: &str,
    revoked_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    sqlx::query(
        "UPDATE role_access_mappings SET status = 'revoked', revoked_at = $1 \
         WHERE role_id = $2 AND bounded_context = $3 AND status = 'active'",
    )
    .bind(revoked_at)
    .bind(role_id)
    .bind(bounded_context)
    .execute(executor)
    .await?;
    Ok(())
}

/// `rule RevokeRole`'s own atomic whole - the role's own status update
/// and every one of its now-`revoked` `RoleAccessMapping`s, in one
/// transaction (drift audit P4 batch: this used to run as N+1 separate
/// autocommit statements - `update_role` then one `revoke_active_role_access_mapping`
/// per mapping - so a crash or a database error partway through could
/// leave a role revoked with some of its own mappings still active, or
/// the reverse. Mirrors the "one transaction, single commit" shape
/// `submit_command` already establishes for the identical reason -
/// `RevokedRoleImpliesMappingsRevoked` now holds for real, not just when
/// nothing goes wrong partway through.
#[tracing::instrument(skip_all)]
pub async fn revoke_role_and_mappings(
    pool: &Pool,
    role: &Role,
    mappings: &[RoleAccessMapping],
) -> crate::error::Result<()> {
    let mut tx = pool.begin().await?;
    update_role(&mut *tx, role).await?;
    for mapping in mappings {
        let revoked_at = mapping
            .revoked_at
            .expect("revoke_role always stamps revoked_at on every mapping it returns");
        revoke_active_role_access_mapping(
            &mut *tx,
            &role.id,
            &mapping.bounded_context.name,
            revoked_at,
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

// --- next_sequence ---

/// Backs the `next_sequence` black box (see the note above rule
/// `CreateExternalEvent` and `event_store::create_external_event`'s own
/// doc comment): allocates the next `Event.sequence` for a bounded
/// context, serialised via the row lock the `UPDATE ... RETURNING`
/// acquires - concurrent callers block on it rather than racing to the
/// same value. The context's own `{schema}.sequence` row is seeded once,
/// at provisioning time (see `provision_bounded_context_schema`), so this
/// is a plain `UPDATE` - no more insert-if-missing step needed now that a
/// bounded context can't exist at all without one.
///
/// Generic over `sqlx::PgExecutor` rather than `&Pool` specifically -
/// every real call site now passes `&mut *tx` from an already-open
/// transaction, so this row's own lock is held for that whole
/// transaction rather than released the instant this one statement's
/// implicit autocommit finishes (see the note above the rules: "locked
/// ... as part of the same transaction that inserts the new Command/
/// Event rows, then incremented and released on commit" - the gaplessness
/// guarantee this gives depends on that, not on this function alone). A
/// bare `&Pool` still works too (autocommit, the pre-fix behaviour) -
/// every direct `db::next_sequence(&pool, ...)` test call keeps compiling
/// unchanged.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn next_sequence<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<i64> {
    let schema = schema_ident(bounded_context);
    let (next,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.sequence SET next_value = next_value + 1 RETURNING next_value"
    )))
    .fetch_one(executor)
    .await?;
    Ok(next)
}

/// `next_sequence`'s own batch form - one command triggering several
/// events (`CommandDecision::Accepted { events }` with more than one
/// `EventSpec`) used to call `next_sequence` once per event, each its own
/// round trip to Postgres, all of them while `submit_command`'s own
/// bounded-context lock is held (Codeberg issue #32: that lock hold
/// duration, not pool size, is what caps real-world write throughput).
/// This claims the whole range in one `UPDATE ... RETURNING`, exactly the
/// same row-lock-based serialisation as the single-value form (already
/// held by the `SELECT ... FOR UPDATE` every real caller takes first), so
/// the semantics are identical - just one round trip for `count` sequence
/// numbers instead of `count` of them. `count == 0` short-circuits without
/// touching the row at all, since `next_value + 0` would still be a wasted
/// round trip for a command whose decision produced no events.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn next_sequence_batch<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
    count: i64,
) -> crate::error::Result<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let schema = schema_ident(bounded_context);
    let (last,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.sequence SET next_value = next_value + $1 RETURNING next_value"
    )))
    .bind(count)
    .fetch_one(executor)
    .await?;
    Ok(((last - count + 1)..=last).collect())
}

/// The highest `sequence` currently committed in a bounded context -
/// `None` when it has no events at all yet. `catch_up_bounded_context`'s
/// own cheap first check every poll tick, so a quiet context (nothing
/// since the last tick) costs one small aggregate query, not a full event
/// reload - `list_events_for_bounded_context_from` only ever runs once
/// this comes back higher than everything that still needs catching up.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn latest_sequence(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(bounded_context);
    let (max,): (Option<i64>,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT MAX(sequence) FROM {schema}.events"
    )))
    .fetch_one(pool)
    .await?;
    Ok(max)
}

// --- Event ---

#[derive(sqlx::FromRow)]
struct EventRow {
    sequence: i64,
    payload: String,
    metadata_type: String,
    metadata_version: i64,
    metadata_client_id: String,
    metadata_created_at: DateTime<Utc>,
    // Codeberg issue #18 - `None` only for a row written before these
    // columns existed.
    metadata_correlation_id: Option<String>,
    metadata_causation_id: Option<String>,
    tags: Json<Vec<Tag>>,
    origin_kind: String,
    origin_source_content: Option<String>,
    origin_source_context: Option<String>,
    origin_command_id: Option<i64>,
}

/// Shared by `EventRow::into_domain` and `list_events_for_bounded_context`'s
/// own row type below - identical origin columns, different row shapes
/// (the latter also selects `event_type_name`, which doesn't fit
/// `EventRow`'s single-event-type-at-a-time contract). Async, unlike
/// every other `*_from_row`/`*_to_str` helper in this module, because
/// `command_triggered` needs a second query - `EventOrigin::CommandTriggered`
/// embeds a whole `Command` by value (see the migration's own doc
/// comment on `commands`), not just an id.
async fn event_origin_from_row(
    pool: &Pool,
    bounded_context: &str,
    kind: &str,
    source_content: Option<String>,
    source_context: Option<String>,
    command_id: Option<i64>,
) -> crate::error::Result<EventOrigin> {
    Ok(match kind {
        "external_triggered" => EventOrigin::ExternalTriggered {
            source_content: source_content
                .expect("external_triggered event row without origin_source_content"),
            source_context,
        },
        "directly_created" => EventOrigin::DirectlyCreated,
        "system_triggered" => EventOrigin::SystemTriggered,
        "command_triggered" => {
            let command_id =
                command_id.expect("command_triggered event row without origin_command_id");
            let command = require_command(pool, bounded_context, command_id).await?;
            EventOrigin::CommandTriggered {
                command: Box::new(command),
            }
        }
        other => panic!("events row has unknown origin_kind {other:?}"),
    })
}

impl EventRow {
    async fn into_domain(
        self,
        pool: &Pool,
        bounded_context: BoundedContext,
        event_type: EventType,
    ) -> crate::error::Result<Event> {
        let origin = event_origin_from_row(
            pool,
            &bounded_context.name,
            &self.origin_kind,
            self.origin_source_content,
            self.origin_source_context,
            self.origin_command_id,
        )
        .await?;
        Ok(Event {
            bounded_context,
            event_type,
            payload: self.payload,
            metadata: Metadata {
                r#type: self.metadata_type,
                version: self.metadata_version,
                client_id: self.metadata_client_id,
                created_at: self.metadata_created_at,
                correlation_id: self.metadata_correlation_id,
                causation_id: self.metadata_causation_id,
            },
            sequence: self.sequence,
            tags: self.tags.0,
            encryption_keys: Vec::new(),
            origin,
        })
    }
}

/// Every `Event` currently stored for one `(bounded_context, event_type)`
/// pair, ordered by `sequence` - the full-snapshot parameter
/// `fetch_events`/`consume_events` each expect (see their own doc
/// comments). `list_events_cached` below is what their own real REST
/// call sites use instead (`crate::event_cache`'s own module doc
/// comment); this function is its unconditional-load fallback, and
/// still used directly wherever the whole type's history is genuinely
/// wanted regardless of recency.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events(
    pool: &Pool,
    bounded_context: &str,
    event_type_name: &str,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;
    let et = require_event_type(pool, bounded_context, event_type_name).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT sequence, payload, metadata_type, metadata_version, metadata_client_id, \
         metadata_created_at, metadata_correlation_id, metadata_causation_id, tags, \
         origin_kind, origin_source_content, origin_source_context, \
         origin_command_id FROM {schema}.events WHERE event_type_name = $1 ORDER BY sequence"
    )))
    .bind(event_type_name)
    .fetch_all(pool)
    .await?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        events.push(row.into_domain(pool, bc.clone(), et.clone()).await?);
    }
    Ok(events)
}

#[derive(sqlx::FromRow)]
struct EventRowAnyType {
    event_type_name: String,
    sequence: i64,
    payload: String,
    metadata_type: String,
    metadata_version: i64,
    metadata_client_id: String,
    metadata_created_at: DateTime<Utc>,
    // Codeberg issue #18 - `None` only for a row written before these
    // columns existed.
    metadata_correlation_id: Option<String>,
    metadata_causation_id: Option<String>,
    tags: Json<Vec<Tag>>,
    origin_kind: String,
    origin_source_content: Option<String>,
    origin_source_context: Option<String>,
    origin_command_id: Option<i64>,
}

/// Every `Event` currently stored for a whole bounded context, across
/// every `EventType`, ordered by `sequence` - the full-snapshot parameter
/// `consistency_boundary_and_matching_events` (and so
/// `event_store::process_command`) expects for `bounded_context_events`,
/// per §1.7's own persistence note. Unlike `list_events` above, a given
/// row's `EventType` isn't known ahead of the query, so each distinct
/// `event_type_name` seen is resolved once and cached for the rest of
/// this call rather than reloaded per row.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events ORDER BY sequence"
    )))
    .fetch_all(pool)
    .await?;

    let mut event_types: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        if !event_types.contains_key(&row.event_type_name) {
            let et = require_event_type(pool, bounded_context, &row.event_type_name).await?;
            event_types.insert(row.event_type_name.clone(), et);
        }
        let origin = event_origin_from_row(
            pool,
            bounded_context,
            &row.origin_kind,
            row.origin_source_content,
            row.origin_source_context,
            row.origin_command_id,
        )
        .await?;
        events.push(Event {
            bounded_context: bc.clone(),
            event_type: event_types[&row.event_type_name].clone(),
            payload: row.payload,
            metadata: Metadata {
                r#type: row.metadata_type,
                version: row.metadata_version,
                client_id: row.metadata_client_id,
                created_at: row.metadata_created_at,
                correlation_id: row.metadata_correlation_id,
                causation_id: row.metadata_causation_id,
            },
            sequence: row.sequence,
            tags: row.tags.0,
            encryption_keys: Vec::new(),
            origin,
        });
    }
    Ok(events)
}

/// Every `Event` committed after `after_sequence`, ordered by `sequence`.
/// `catch_up_bounded_context`'s own bounded load, unlike
/// `list_events_for_bounded_context` above's "always reload everything"
/// shape. Same per-row resolution as that function, just with a `WHERE`
/// clause and a starting point.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context_from(
    pool: &Pool,
    bounded_context: &str,
    after_sequence: i64,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         WHERE sequence > $1 ORDER BY sequence"
    )))
    .bind(after_sequence)
    .fetch_all(pool)
    .await?;

    let mut event_types: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        if !event_types.contains_key(&row.event_type_name) {
            let et = require_event_type(pool, bounded_context, &row.event_type_name).await?;
            event_types.insert(row.event_type_name.clone(), et);
        }
        let origin = event_origin_from_row(
            pool,
            bounded_context,
            &row.origin_kind,
            row.origin_source_content,
            row.origin_source_context,
            row.origin_command_id,
        )
        .await?;
        events.push(Event {
            bounded_context: bc.clone(),
            event_type: event_types[&row.event_type_name].clone(),
            payload: row.payload,
            metadata: Metadata {
                r#type: row.metadata_type,
                version: row.metadata_version,
                client_id: row.metadata_client_id,
                created_at: row.metadata_created_at,
                correlation_id: row.metadata_correlation_id,
                causation_id: row.metadata_causation_id,
            },
            sequence: row.sequence,
            tags: row.tags.0,
            encryption_keys: Vec::new(),
            origin,
        });
    }
    Ok(events)
}

/// `list_events_for_bounded_context_from`'s own capped sibling -
/// Codeberg issue #25's `catch_up_partitioned_projection` is the one
/// caller (`MAX_EVENTS_PER_PARTITION_TICK`'s own doc comment explains
/// why it needs a cap that function's other caller, `catch_up_snapshots`,
/// doesn't). A separate function rather than an optional `limit`
/// parameter added to `list_events_for_bounded_context_from` itself, so
/// that function's existing, unrelated caller stays untouched.
pub async fn list_events_for_bounded_context_from_limited(
    pool: &Pool,
    bounded_context: &str,
    after_sequence: i64,
    limit: i64,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         WHERE sequence > $1 ORDER BY sequence LIMIT $2"
    )))
    .bind(after_sequence)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut event_types: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        if !event_types.contains_key(&row.event_type_name) {
            let et = require_event_type(pool, bounded_context, &row.event_type_name).await?;
            event_types.insert(row.event_type_name.clone(), et);
        }
        let origin = event_origin_from_row(
            pool,
            bounded_context,
            &row.origin_kind,
            row.origin_source_content,
            row.origin_source_context,
            row.origin_command_id,
        )
        .await?;
        events.push(Event {
            bounded_context: bc.clone(),
            event_type: event_types[&row.event_type_name].clone(),
            payload: row.payload,
            metadata: Metadata {
                r#type: row.metadata_type,
                version: row.metadata_version,
                client_id: row.metadata_client_id,
                created_at: row.metadata_created_at,
                correlation_id: row.metadata_correlation_id,
                causation_id: row.metadata_causation_id,
            },
            sequence: row.sequence,
            tags: row.tags.0,
            encryption_keys: Vec::new(),
            origin,
        });
    }
    Ok(events)
}

/// Tag-indexed sibling of `list_events_for_bounded_context`/`_from` -
/// [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 1" fix. Those two always fetch
/// the *whole* bounded context and leave tag-matching to the caller's
/// own in-memory filter (`consistency_boundary_and_matching_events`'s
/// own union semantics) - fine while a bounded context is small, but a
/// full unfiltered scan on every command submission once it isn't. This
/// pushes the same union-of-tags matching down into the query itself,
/// via `events_by_tags` (a GIN index on the already-`JSONB` `tags`
/// column, added in `provision_bounded_context_schema`) - one `tags @>
/// $N::jsonb` containment clause per wanted tag, `OR`'d together,
/// optionally further bounded by `sequence > after_sequence` (folds in
/// both `queryEvents`' own pagination and `submit_command`'s
/// DCB-conflict redispatch check, which needs the identical "tag-scoped
/// and newer than X" shape - see that function's own call site).
///
/// An empty `tags` slice returns `Ok(vec![])` without ever touching
/// Postgres - mirrors `consistency_boundary_and_matching_events`'s own
/// "no `tag_mappings` declared" case exactly, so a `CommandType`/
/// `EventType` with no tags sees no behavioural change at all.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context_matching_tags(
    pool: &Pool,
    bounded_context: &str,
    tags: &[Tag],
    after_sequence: Option<i64>,
) -> crate::error::Result<Vec<Event>> {
    if tags.is_empty() {
        return Ok(Vec::new());
    }

    let bc = require_bounded_context(pool, bounded_context).await?;
    list_events_for_bounded_context_matching_tags_with_bc(pool, &bc, tags, after_sequence, None)
        .await
}

/// The same query [`list_events_for_bounded_context_matching_tags`] runs,
/// for a caller that already has the `BoundedContext` row in hand and
/// would otherwise be re-fetching it redundantly - `submit_one_command_in_tx`'s
/// own per-command DCB-conflict delta check is exactly that caller: every
/// command in a `CommandBatcher` batch shares the identical bounded
/// context (that's what the batch's own queue is keyed on), and
/// `command_type.bounded_context` is already that same row, loaded once
/// when the command type itself was resolved - re-fetching it (plus,
/// transitively, `get_role` for its `created_by_role_id`) once per
/// command in a large, self-tuning batch was pure round-trip waste sitting
/// inside the batch leader's own held lock.
async fn list_events_for_bounded_context_matching_tags_with_bc(
    pool: &Pool,
    bc: &BoundedContext,
    tags: &[Tag],
    after_sequence: Option<i64>,
    known_event_types: Option<&std::collections::HashMap<String, EventType>>,
) -> crate::error::Result<Vec<Event>> {
    if tags.is_empty() {
        return Ok(Vec::new());
    }

    let bounded_context = bc.name.as_str();
    let schema = schema_ident(bounded_context);
    // One `[Tag]`-shaped single-element JSONB array literal per wanted
    // tag - containment (`@>`) needs the right-hand side to be an array
    // too, since `tags` itself is stored as an array of tag objects, not
    // one bare object.
    let tag_literals: Vec<String> = tags
        .iter()
        .map(|t| {
            serde_json::to_string(std::slice::from_ref(t)).expect("Tag serialisation is infallible")
        })
        .collect();
    let tag_clause = (1..=tag_literals.len())
        .map(|i| format!("tags @> ${i}::jsonb"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let sequence_param = tag_literals.len() + 1;
    let where_clause = match after_sequence {
        Some(_) => format!("({tag_clause}) AND sequence > ${sequence_param}"),
        None => tag_clause,
    };

    let sql = format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         WHERE {where_clause} ORDER BY sequence"
    );
    let mut query = sqlx::query_as::<_, EventRowAnyType>(sqlx::AssertSqlSafe(sql));
    for literal in tag_literals {
        query = query.bind(literal);
    }
    if let Some(after_sequence) = after_sequence {
        query = query.bind(after_sequence);
    }
    let rows: Vec<EventRowAnyType> = query.fetch_all(pool).await?;

    let row_count = rows.len();
    let row_loop_started = std::time::Instant::now();

    // Every distinct event type this result set needs that
    // `known_event_types` (a caller's already-warmed map, e.g.
    // `decide_command_in_tx`'s own) doesn't already answer, batched into
    // one `WHERE name = ANY($1)` round trip - `get_commands_by_ids_with_bc`'s
    // own precedent below, same reasoning: a batched query, not a
    // concurrent burst of small ones (a first, concurrent attempt at
    // fixing this whole row-processing loop measured worse overall, by
    // starving the next batch's own lock acquisition - see this
    // function's own doc comment for the full story).
    let mut event_types: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut missing_type_names: Vec<&str> = Vec::new();
    for row in &rows {
        if event_types.contains_key(&row.event_type_name) {
            continue;
        }
        match known_event_types.and_then(|m| m.get(&row.event_type_name)) {
            Some(et) => {
                event_types.insert(row.event_type_name.clone(), et.clone());
            }
            None => {
                if !missing_type_names.contains(&row.event_type_name.as_str()) {
                    missing_type_names.push(&row.event_type_name);
                }
            }
        }
    }
    let event_type_lookup_started = std::time::Instant::now();
    event_types.extend(get_event_types_by_names_with_bc(pool, bc, &missing_type_names).await?);
    let event_type_lookup_elapsed = event_type_lookup_started.elapsed();

    // The real cost this whole investigation found (docs/architecture.md
    // §61's round four): a `command_triggered` row's own origin needs the
    // *entire* originating `Command` (`get_command_by_id`, itself several
    // round trips deep - `get_command_type` → `get_bounded_context` →
    // `get_role`). A first attempt fetched these concurrently, one
    // `event_origin_from_row` future per row - measured *worse*, not
    // better: bursting many simultaneous connection requests out of a
    // shared, already-contended pool starved the *next* batch's own lock
    // acquisition (`command batch leader lock wait` climbed to ~1.7s mean
    // in that measurement). This instead batches the distinct
    // `origin_command_id`s this result set actually needs into one
    // `WHERE id = ANY($1)` query (`get_commands_by_ids_with_bc`, itself
    // internally batching the distinct `command_type_name`s the same
    // way) - one or two round trips total for the whole row set, not one
    // per row, concurrent or not.
    let origin_lookup_started = std::time::Instant::now();
    let mut command_ids: Vec<i64> = Vec::new();
    for row in &rows {
        if row.origin_kind == "command_triggered" {
            if let Some(id) = row.origin_command_id {
                if !command_ids.contains(&id) {
                    command_ids.push(id);
                }
            }
        }
    }
    let commands_by_id = get_commands_by_ids_with_bc(pool, bc, &command_ids).await?;
    let mut origins = Vec::with_capacity(rows.len());
    for row in &rows {
        origins.push(match row.origin_kind.as_str() {
            "external_triggered" => EventOrigin::ExternalTriggered {
                source_content: row
                    .origin_source_content
                    .clone()
                    .expect("external_triggered event row without origin_source_content"),
                source_context: row.origin_source_context.clone(),
            },
            "directly_created" => EventOrigin::DirectlyCreated,
            "system_triggered" => EventOrigin::SystemTriggered,
            "command_triggered" => {
                let command_id = row
                    .origin_command_id
                    .expect("command_triggered event row without origin_command_id");
                // `commands_by_id` came from `get_commands_by_ids_with_bc`
                // (see its own sibling note in `get_commands_by_ids_with_bc`
                // above) - a `bc`-in-hand, no-recheck batch query, so a
                // `hard_delete_bounded_context` race surfaces as a real
                // `Err` before this lookup, not a silent miss. A genuine
                // miss here is a real bug (an `events` row outliving the
                // `commands` row it names), not a race.
                let command = commands_by_id
                    .get(&command_id)
                    .expect("events row references a commands row that no longer exists");
                EventOrigin::CommandTriggered {
                    command: Box::new(command.clone()),
                }
            }
            other => panic!("events row has unknown origin_kind {other:?}"),
        });
    }
    let origin_lookup_elapsed = origin_lookup_started.elapsed();

    let mut events = Vec::with_capacity(rows.len());
    for (row, origin) in rows.into_iter().zip(origins) {
        events.push(Event {
            bounded_context: bc.clone(),
            event_type: event_types[&row.event_type_name].clone(),
            payload: row.payload,
            metadata: Metadata {
                r#type: row.metadata_type,
                version: row.metadata_version,
                client_id: row.metadata_client_id,
                created_at: row.metadata_created_at,
                correlation_id: row.metadata_correlation_id,
                causation_id: row.metadata_causation_id,
            },
            sequence: row.sequence,
            tags: row.tags.0,
            encryption_keys: Vec::new(),
            origin,
        });
    }
    tracing::info!(
        row_count,
        row_loop_us = row_loop_started.elapsed().as_micros(),
        event_type_lookup_us = event_type_lookup_elapsed.as_micros(),
        origin_lookup_us = origin_lookup_elapsed.as_micros(),
        "delta query row-processing loop"
    );
    Ok(events)
}

/// One `Event` by its own `sequence` - `InspectEvent`'s own lookup key
/// (`context event: Event`), added propagating `skilj-graphql`'s
/// `EventQuery` resolver (Phase 3). `None` when no event in this bounded
/// context has that sequence.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_event_by_sequence(
    pool: &Pool,
    bounded_context: &str,
    sequence: i64,
) -> crate::error::Result<Option<Event>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };

    let schema = schema_ident(bounded_context);
    let Some(row): Option<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events WHERE sequence = $1"
    )))
    .bind(sequence)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };

    let et = require_event_type(pool, bounded_context, &row.event_type_name).await?;
    let origin = event_origin_from_row(
        pool,
        bounded_context,
        &row.origin_kind,
        row.origin_source_content,
        row.origin_source_context,
        row.origin_command_id,
    )
    .await?;
    Ok(Some(Event {
        bounded_context: bc,
        event_type: et,
        payload: row.payload,
        metadata: Metadata {
            r#type: row.metadata_type,
            version: row.metadata_version,
            client_id: row.metadata_client_id,
            created_at: row.metadata_created_at,
            correlation_id: row.metadata_correlation_id,
            causation_id: row.metadata_causation_id,
        },
        sequence: row.sequence,
        tags: row.tags.0,
        encryption_keys: Vec::new(),
        origin,
    }))
}

/// The most recent `limit` events of a whole bounded context, ascending
/// by `sequence` - `event_cache::EventCache::warm`'s own fetch, and the
/// only place this module issues a `DESC ... LIMIT` query at all (every
/// other listing function loads a range in ascending order directly).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_recent_events_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
    limit: usize,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         ORDER BY sequence DESC LIMIT $1"
    )))
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    let mut event_types: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        if !event_types.contains_key(&row.event_type_name) {
            let et = require_event_type(pool, bounded_context, &row.event_type_name).await?;
            event_types.insert(row.event_type_name.clone(), et);
        }
        let origin = event_origin_from_row(
            pool,
            bounded_context,
            &row.origin_kind,
            row.origin_source_content,
            row.origin_source_context,
            row.origin_command_id,
        )
        .await?;
        events.push(Event {
            bounded_context: bc.clone(),
            event_type: event_types[&row.event_type_name].clone(),
            payload: row.payload,
            metadata: Metadata {
                r#type: row.metadata_type,
                version: row.metadata_version,
                client_id: row.metadata_client_id,
                created_at: row.metadata_created_at,
                correlation_id: row.metadata_correlation_id,
                causation_id: row.metadata_causation_id,
            },
            sequence: row.sequence,
            tags: row.tags.0,
            encryption_keys: Vec::new(),
            origin,
        });
    }
    // The query above fetched newest-first to make LIMIT cheap - reverse
    // back to the ascending order every other listing function, and
    // EventCache's own window, expects.
    events.reverse();
    Ok(events)
}

/// The type-scoped, `after_sequence`-bounded twin of `list_events` above -
/// `list_events_cached`'s own fallback path, so falling back to Postgres
/// doesn't itself load more than the request actually needs.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_from(
    pool: &Pool,
    bounded_context: &str,
    event_type_name: &str,
    after_sequence: i64,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;
    let et = require_event_type(pool, bounded_context, event_type_name).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT sequence, payload, metadata_type, metadata_version, metadata_client_id, \
         metadata_created_at, metadata_correlation_id, metadata_causation_id, tags, \
         origin_kind, origin_source_content, origin_source_context, \
         origin_command_id FROM {schema}.events WHERE event_type_name = $1 AND sequence > $2 \
         ORDER BY sequence"
    )))
    .bind(event_type_name)
    .bind(after_sequence)
    .fetch_all(pool)
    .await?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        events.push(row.into_domain(pool, bc.clone(), et.clone()).await?);
    }
    Ok(events)
}

/// `FetchEvents`/`ConsumeEvents`'s own real read path - see
/// `crate::event_cache`'s own module doc comment for the full design.
/// Tries the cache first; `list_events_from` above is the fallback, so a
/// coverage miss still only loads what the request actually needs, not
/// the whole type's history.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_cached(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    event_type_name: &str,
    after_sequence: i64,
) -> crate::error::Result<Vec<Event>> {
    match cache
        .try_events_after(pool, bounded_context, after_sequence)
        .await?
    {
        Some(events) => Ok(events
            .into_iter()
            .filter(|e| e.event_type.name == event_type_name)
            .collect()),
        None => list_events_from(pool, bounded_context, event_type_name, after_sequence).await,
    }
}

/// `QueryEvents`/`CountEvents`'s own real read path when no `tags`
/// filter is given - see `crate::event_cache`'s own module doc comment
/// for the full design. `after_sequence: -1` asks for full history
/// (`CountEvents`, which has no `after_sequence` of its own);
/// `list_events_for_bounded_context`/`list_events_for_bounded_context_from`
/// (both already existed, reused unchanged) are the fallback for either
/// case respectively. `ProcessCommand`'s own DCB pre-check, and
/// `QueryEvents`/`CountEvents` when a `tags` filter *is* given, use
/// `list_events_for_bounded_context_matching_tags_cached` below instead -
/// [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 1" fix, avoiding exactly the
/// full-bounded-context fetch this function's own fallback still does.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context_cached(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    after_sequence: i64,
) -> crate::error::Result<Vec<Event>> {
    match cache
        .try_events_after(pool, bounded_context, after_sequence)
        .await?
    {
        Some(events) => Ok(events),
        None if after_sequence < 0 => list_events_for_bounded_context(pool, bounded_context).await,
        None => list_events_for_bounded_context_from(pool, bounded_context, after_sequence).await,
    }
}

/// `ProcessCommand`'s own DCB pre-check, and `QueryEvents`/`CountEvents`
/// when a `tags` filter is supplied - the cached counterpart to
/// `list_events_for_bounded_context_matching_tags` above, same
/// cache-first/Postgres-fallback shape `list_events_for_bounded_context_cached`
/// already has, via `EventCache::try_events_matching_tags` instead of
/// `try_events_after`. Already tag-scoped either way - see that
/// function's own doc comment.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context_matching_tags_cached(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    tags: &[Tag],
    after_sequence: Option<i64>,
) -> crate::error::Result<Vec<Event>> {
    match cache
        .try_events_matching_tags(pool, bounded_context, tags)
        .await?
    {
        Some(events) => Ok(events
            .into_iter()
            .filter(|e| e.sequence > after_sequence.unwrap_or(-1))
            .collect()),
        None => {
            list_events_for_bounded_context_matching_tags(
                pool,
                bounded_context,
                tags,
                after_sequence,
            )
            .await
        }
    }
}

/// `InspectEvent`'s own real read path - see `crate::event_cache`'s own
/// module doc comment for the full design.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_event_by_sequence_cached(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    sequence: i64,
) -> crate::error::Result<Option<Event>> {
    match cache
        .try_event_by_sequence(pool, bounded_context, sequence)
        .await?
    {
        Some(event) => Ok(Some(event)),
        None => get_event_by_sequence(pool, bounded_context, sequence).await,
    }
}

/// `command_id` must be `Some` exactly when `event.origin` is
/// `CommandTriggered`, and `None` otherwise - the caller's own
/// `insert_command(pool, &result.command)` (returning the new row's id)
/// runs first for a `ProcessCommandResult`, then this is called once per
/// produced `Event` with that same id.
#[tracing::instrument(skip_all)]
pub async fn insert_event<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    event: &Event,
    command_id: Option<i64>,
) -> crate::error::Result<()> {
    let (origin_kind, source_content, source_context) = match &event.origin {
        EventOrigin::ExternalTriggered {
            source_content,
            source_context,
        } => {
            debug_assert!(command_id.is_none());
            (
                "external_triggered",
                Some(source_content.clone()),
                source_context.clone(),
            )
        }
        EventOrigin::DirectlyCreated => {
            debug_assert!(command_id.is_none());
            ("directly_created", None, None)
        }
        EventOrigin::SystemTriggered => {
            debug_assert!(command_id.is_none());
            ("system_triggered", None, None)
        }
        EventOrigin::CommandTriggered { .. } => {
            debug_assert!(
                command_id.is_some(),
                "insert_event: a CommandTriggered origin needs command_id - call \
                 insert_command(pool, &result.command) first and pass its returned id"
            );
            ("command_triggered", None, None)
        }
    };

    let schema = schema_ident(&event.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.events (sequence, event_type_name, payload, metadata_type, \
         metadata_version, metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, \
         origin_kind, origin_source_content, origin_source_context, origin_command_id) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)"
    )))
    .bind(event.sequence)
    .bind(&event.event_type.name)
    .bind(&event.payload)
    .bind(&event.metadata.r#type)
    .bind(event.metadata.version)
    .bind(&event.metadata.client_id)
    .bind(event.metadata.created_at)
    .bind(&event.metadata.correlation_id)
    .bind(&event.metadata.causation_id)
    .bind(Json(&event.tags))
    .bind(origin_kind)
    .bind(source_content)
    .bind(source_context)
    .bind(command_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// The real, transactional half of [§8](../../../docs/architecture.md#open-for-a-future-pass) item 6: inserts `event` and, in
/// the *same* transaction, folds it into every `sync = true` `Projection`
/// in its bounded context via `dispatcher` - "commits or fails with the
/// write" (the note above the rules), the whole reason sync projections
/// exist as a distinct, deliberately-friction-laden opt-in over the
/// async default. Every event-creation call site (`CreateExternalEvent`/
/// `CreateDirectEvent`'s REST handlers, `ProcessCommand`'s REST and
/// GraphQL handlers alike) calls this instead of the plain `insert_event`
/// above - which stays exactly as it is, still the right shape for a
/// bounded context with no sync projections at all (every event-writing
/// test fixture so far) and for `insert_event`'s own use inside this
/// function.
///
/// `SELECT ... FOR UPDATE` on each sync projection's own `projection_state`
/// row is what makes this safe under concurrent writers to the same
/// projection - the identical row-lock-based serialisation
/// `next_sequence` already relies on for sequence assignment, just one
/// row per sync projection instead of one row per bounded context.
/// `caught_up_to` advances for every sync projection in the context
/// regardless of whether it actually consumes this event's type (the
/// note above the rules: "advances that projection's own caught_up_to...
/// either way") - `dispatcher.project()` itself is what decides whether
/// the *state* changes; this function always bumps the position.
#[tracing::instrument(skip_all)]
pub async fn insert_event_and_update_sync_projections(
    pool: &Pool,
    event: &Event,
    command_id: Option<i64>,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    encryption_key_ids: &[i64],
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
) -> crate::error::Result<()> {
    let sync_projections =
        sync_projections_for_bounded_context(pool, &event.bounded_context.name).await?;
    let mut tx = pool.begin().await?;
    insert_event_and_update_sync_projections_in_tx(
        &mut tx,
        event,
        command_id,
        dispatcher,
        encryption_key_ids,
        &sync_projections,
    )
    .await?;
    tx.commit().await?;

    // EventSubscription's own real-time delivery - after the commit, not
    // before: a subscriber must never see an event that could still have
    // rolled back (see EventBroadcaster's own doc comment for why this
    // is the one choke point every event-creation call site already
    // shares).
    broadcaster.publish(event);
    record_event_appended(event);
    notify_event_appended(pool, event, broadcaster.instance_id()).await;
    event_cache.append(event).await;

    Ok(())
}

/// The transactional core of `insert_event_and_update_sync_projections`
/// above, split out so `next_sequence`'s own lock, this event's insert,
/// and (for `ProcessCommand`) the triggering `Command`'s own insert can
/// all share one already-open transaction instead of each getting their
/// own - see the note above the rules in specs/skilj.allium: "a single
/// row per bounded context... is locked... as part of the same
/// transaction that inserts the new Command/Event rows, then incremented
/// and released on commit." Neither commits `tx` nor broadcasts - both
/// stay the caller's job, exactly once, after every event in a single
/// submission (a `ProcessCommand` call can trigger several) has been
/// folded in.
///
/// `sync_projections` is the caller's job too now (Codeberg issue #32) -
/// this function used to re-run `list_projections_for_bounded_context`'s
/// own metadata read itself, once per event, which meant a command that
/// decided several events paid for it again and again while
/// `submit_command`'s own bounded-context lock was held. Every real call
/// site now fetches it exactly once via `sync_projections_for_bounded_context`
/// and passes the same slice into every event this one submission
/// inserts. That fetch is still a plain `pool` read, not `tx` - deliberately,
/// the same "small, admin-managed list, not worth locking" treatment
/// `list_projections_for_bounded_context`'s own callers already give it
/// elsewhere - but it is only safe to run *after* the caller's own
/// `next_sequence`/`SELECT ... FOR UPDATE` on this bounded context's
/// `sequence` row has already been taken (every real call site fetches it
/// no earlier than that point): `promote_projection_rebuild` - the one
/// place a projection's own `sync` flag can flip mid-flight - takes that
/// identical lock before it can promote (drift audit finding #6, see
/// project memory `skilj-drift-audit-2026-08-20`, and that function's own
/// doc comment), so the two can never interleave once this caller's own
/// lock is held, and there is no possible half-visible state left to see.
/// Fetching it before that lock would reopen exactly that race.
#[tracing::instrument(skip_all)]
pub async fn insert_event_and_update_sync_projections_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
    command_id: Option<i64>,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    encryption_key_ids: &[i64],
    sync_projections: &[Projection],
) -> crate::error::Result<()> {
    let bounded_context = &event.bounded_context.name;
    let schema = schema_ident(bounded_context);

    insert_event(&mut **tx, event, command_id).await?;

    // `event.encryption_keys`' own `id`s aren't carried on the domain
    // struct (it has none, matching `entity EncryptionKey` itself) -
    // `encryption_key_ids` is what `get_or_create_encryption_key` handed
    // the caller back alongside it, threaded through here so this
    // transaction can also link the join rows atomically with the event
    // insert they belong to.
    // Same `unnest`-batched insert as `insert_command`'s own
    // `command_encryption_keys` above, same reasoning - one round trip
    // for however many keys this event's payload needed, not one per
    // key.
    if !encryption_key_ids.is_empty() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.event_encryption_keys (event_sequence, encryption_key_id) \
             SELECT $1, unnest($2::bigint[])"
        )))
        .bind(event.sequence)
        .bind(encryption_key_ids)
        .execute(&mut **tx)
        .await?;
    }

    for projection in sync_projections {
        // `Some(vec![])` (registered, but this event's type isn't
        // consumed) and `None` (dispatcher doesn't recognise this
        // projection at all) both fall through to an empty loop below -
        // zero instances touched either way, `caught_up_to` still
        // advances unconditionally after it (§9's "keyed / multi-row
        // Projections" pass).
        let keys = dispatcher
            .keys(bounded_context, &projection.name, event)
            .unwrap_or_default();
        let default_state_json = dispatcher
            .default_state(bounded_context, &projection.name)
            .unwrap_or_default();
        let owner_tag_key = dispatcher
            .owner_tag_key(bounded_context, &projection.name)
            .flatten();

        for key in &keys {
            // `as_of_sequence` guard (Codeberg issue #25's investigation
            // finding) - unreachable in practice on this particular path
            // (every call here shares the one transaction that also
            // holds `insert_event`'s own bounded-context `sequence` row
            // lock, already fully serializing concurrent instances for
            // the whole bc, per `insert_event_via_the_locked_path`'s own
            // doc comment), kept anyway so this call site stays
            // structurally identical to the two genuinely-concurrent
            // ones below rather than being the one exception mid-fix.
            let (as_of_sequence, current_state) = get_or_create_projection_state_for_update(
                &mut **tx,
                &schema,
                &projection.name,
                key,
                &default_state_json,
            )
            .await?;
            if as_of_sequence >= event.sequence {
                continue;
            }

            let new_state = match dispatcher.project(
                bounded_context,
                &projection.name,
                &current_state,
                event,
                key,
            ) {
                Some(result) => result?,
                None => current_state,
            };

            apply_projection_fold_update(
                &mut **tx,
                &schema,
                "projection_state",
                "",
                &projection.name,
                key,
                &new_state,
                owner_tag_key,
                event,
            )
            .await?;
        }

        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.projections SET caught_up_to = $1 WHERE name = $2"
        )))
        .bind(event.sequence)
        .bind(&projection.name)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

/// The `dedupe_partition_key`/`dedupe_sequence` pair from
/// specs/skilj.allium's own `SubmitExternalEvent` trigger
/// (`rule CreateExternalEvent`, [docs/architecture.md §39](../../../docs/architecture.md#external-message-dedup-create-external-event)) - always
/// supplied together or not at all. That "both or neither" `requires`
/// guard in the spec is satisfied structurally here, not by a separate
/// runtime check: there is no way to construct one of these fields
/// without the other through this type (or, at the REST wire boundary,
/// through `ExternalEventRequest`'s own `dedupe: Option<DedupeRequest>`
/// shape) - a caller supplying exactly one on the wire fails ordinary
/// JSON deserialization before this type is ever built, the same
/// "malformed request, not this crate's rejection to model" register
/// any other structurally-invalid request body already gets.
pub struct DedupeCursor<'a> {
    /// Names the partition, shard, or stream this message came from -
    /// e.g. `"{topic}:{partition}"` for Kafka, a shard id for Kinesis.
    pub partition_key: &'a str,
    /// This message's own position within `partition_key` - must be
    /// strictly increasing per partition for the caller's own external
    /// source, the property this whole mechanism relies on and cannot
    /// verify itself (see `highest_dedupe_sequence`'s own doc comment).
    pub sequence: i64,
}

/// What `create_and_insert_external_event` settles on - either a real
/// insert (`Created`, already persisted by the time this returns) or a
/// redelivery this rule recognises and accepts without creating anything
/// (`Redelivered`). Not `SubmitCommandOutcome`'s two-way split
/// (`Accepted`/`Deduplicated`) reused, deliberately: a genuine business
/// rejection has no equivalent here (`create_external_event`'s own
/// failure modes - `TokenNotActive`, `ExternalCreationNotAllowed`, a bad
/// payload - are real errors, returned as `Err`, not a third outcome
/// variant), and calling this case "Deduplicated" would invite comparing
/// it to `SubmitCommandOutcome::Deduplicated`, which returns the
/// original outcome's own triggered sequences - this outcome carries
/// nothing, on purpose, since a watermark remembers only the highest
/// sequence seen, not which event any particular past message produced.
/// See specs/skilj.allium's own `ExternalEventIngestion.ARedeliveryProducesNoEventAndNoOutcome`.
#[derive(Debug)]
pub enum CreateExternalEventOutcome {
    Created(Box<Event>),
    Redelivered,
}

/// `highest_dedupe_sequence(adapter, dedupe_partition_key)` from
/// specs/skilj.allium's own `rule CreateExternalEvent` - a black box in
/// the same register as `next_sequence`/`recorded_acceptance`: durable
/// state this library owns, whose storage shape the spec doesn't reach
/// into. `None` when nothing has been recorded yet for this
/// `(adapter_id, partition_key)` pair - the spec's own "or when the
/// partition key is null" clause is handled by the caller never calling
/// this at all when `dedupe` is `None`, not by this function.
///
/// Must be called after the bounded context's own `sequence` row lock is
/// already held (`next_sequence`, inside `create_and_insert_external_event`'s
/// own transaction) - the identical "no lock of its own, rides on the
/// one already held" register `lookup_idempotency_key` already uses, for
/// the same reason: every write into this bounded context is already
/// serialised by that lock, so a plain, unlocked `SELECT` here is
/// race-free.
async fn highest_dedupe_sequence<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    schema: &str,
    adapter_id: &str,
    partition_key: &str,
) -> crate::error::Result<Option<i64>> {
    let row: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT last_sequence FROM {schema}.external_message_cursors \
         WHERE adapter_id = $1 AND partition_key = $2"
    )))
    .bind(adapter_id)
    .bind(partition_key)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|(seq,)| seq))
}

/// Records `sequence` as the new watermark for `(adapter_id,
/// partition_key)`, inside the same transaction as the `Event` it
/// stands for - a watermark advances exactly when the event it
/// represents is durable, never before and never without it (see
/// `create_and_insert_external_event`'s own doc comment). `ON CONFLICT
/// ... DO UPDATE`, not a plain insert the way `insert_idempotency_key`
/// is - unlike that table, a given `(adapter_id, partition_key)` pair is
/// expected to be written many times over its life, once per message
/// from a real partition, not once ever.
async fn advance_dedupe_watermark<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    schema: &str,
    adapter_id: &str,
    partition_key: &str,
    sequence: i64,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.external_message_cursors \
         (adapter_id, partition_key, last_sequence, updated_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (adapter_id, partition_key) \
         DO UPDATE SET last_sequence = EXCLUDED.last_sequence, updated_at = EXCLUDED.updated_at"
    )))
    .bind(adapter_id)
    .bind(partition_key)
    .bind(sequence)
    .bind(now)
    .execute(executor)
    .await?;
    Ok(())
}

/// `CreateExternalEvent`'s own atomic whole: `next_sequence`'s row lock,
/// `event_store::create_external_event`'s pure construction (which needs
/// that lock's own allocated sequence baked into the `Event` it builds),
/// and the insert itself, all inside one transaction - `skilj-rest`'s
/// `post_events_external` handler used to run these three steps
/// unlocked/separately-transacted; a rejection from
/// `create_external_event` (`TokenNotActive`/`ExternalCreationNotAllowed`)
/// after `next_sequence` had already run on the bare pool used to burn a
/// sequence number for an event that was never written -
/// `SequenceIsGaplessPerBoundedContext`'s actual bug. Here, that same
/// rejection instead rolls `tx` back before it ever commits, so the
/// allocation never happened as far as any other reader can tell.
/// `resolve_encryption_keys` still runs on `pool`, before `tx` opens -
/// deliberately, not a gap: see the note above the rules in
/// specs/skilj.allium for why `EncryptionKey` provisioning is resolved
/// before a write's own transaction rather than inside it (real work -
/// a master-key wrap, a round trip - that shouldn't extend how long a
/// row lock like `next_sequence`'s own is held). Not callable from
/// anywhere but `skilj-rest` today (no
/// GraphQL surface offers `CreateExternalEvent`), but lives here rather
/// than in that crate per docs/architecture.md §3.1/§3.2: `skilj-core`
/// is the only crate that owns the database driver, so no other crate
/// ever opens a `Transaction` itself.
///
/// **`dedupe`** ([docs/architecture.md §39](../../../docs/architecture.md#external-message-dedup-create-external-event), specs/skilj.allium's own
/// `rule CreateExternalEvent`): `None` reproduces every existing
/// caller's own behaviour exactly, byte for byte - no lookup, no write
/// to `external_message_cursors`, an event created every single time,
/// the same "omitting it changes nothing" guarantee `submit_command`'s
/// own `idempotency_key: None` already gives (see
/// `ExternalEventIngestion.OmittingTheDedupePairChangesNothing`).
/// `Some(cursor)`: checked against `highest_dedupe_sequence` right after
/// `next_sequence`'s own lock is acquired - the earliest point that's
/// race-free, mirroring exactly where `submit_command`'s own idempotency
/// check runs relative to that same lock. A watermark hit rolls `tx`
/// back (dropped, nothing else was written) and returns
/// `CreateExternalEventOutcome::Redelivered` without ever calling
/// `event_store::create_external_event` at all; a miss proceeds exactly
/// as `None` would, plus one more write in the same transaction -
/// `advance_dedupe_watermark` - before `tx.commit()`.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all)]
pub async fn create_and_insert_external_event(
    pool: &Pool,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    adapter: &ExternalEventToken,
    payload: String,
    source_content: String,
    source_context: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    dedupe: Option<DedupeCursor<'_>>,
    now: DateTime<Utc>,
    encryption_master_key: Option<&EncryptionMasterKey>,
) -> crate::error::Result<CreateExternalEventOutcome> {
    let bounded_context_name = adapter.event_type.bounded_context.name.clone();
    let schema = schema_ident(&bounded_context_name);

    let mut resolved = std::collections::HashMap::new();
    resolve_encryption_keys(
        pool,
        &bounded_context_name,
        &adapter.event_type.sensitive_fields,
        &payload,
        encryption_master_key,
        &mut resolved,
    )
    .await?;

    let mut tx = pool.begin().await?;
    let next_seq = next_sequence(&mut *tx, &bounded_context_name).await?;

    if let Some(cursor) = &dedupe {
        let watermark =
            highest_dedupe_sequence(&mut *tx, &schema, &adapter.id, cursor.partition_key).await?;
        if watermark.is_some_and(|w| cursor.sequence <= w) {
            // Implicit rollback - nothing else was written, the same
            // "a hit is a cached prior answer, not a new decision, tx is
            // simply dropped" treatment `submit_command`'s own
            // idempotency-key check already uses.
            return Ok(CreateExternalEventOutcome::Redelivered);
        }
    }

    let event = crate::event_store::create_external_event(
        adapter,
        payload,
        source_content,
        source_context,
        correlation_id,
        causation_id,
        next_seq,
        now,
        |subject_key, subject_value| {
            let (key, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect(
                    "resolve_encryption_keys pre-resolved every subject sensitive_field_subjects \
                     named",
                );
            (key.clone(), data_key.clone())
        },
    )?;
    let encryption_key_ids = encryption_key_ids(&event.encryption_keys, &resolved);
    // Fetched only now - after `next_sequence` above already took this
    // bounded context's own lock - see `insert_event_and_update_sync_projections_in_tx`'s
    // own doc comment on why that ordering, not "as early as possible", is
    // what keeps this read race-free against `promote_projection_rebuild`.
    let sync_projections =
        sync_projections_for_bounded_context(pool, &bounded_context_name).await?;
    insert_event_and_update_sync_projections_in_tx(
        &mut tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
        &sync_projections,
    )
    .await?;
    if let Some(cursor) = &dedupe {
        advance_dedupe_watermark(
            &mut *tx,
            &schema,
            &adapter.id,
            cursor.partition_key,
            cursor.sequence,
            now,
        )
        .await?;
    }
    tx.commit().await?;
    broadcaster.publish(&event);
    record_event_appended(&event);
    notify_event_appended(pool, &event, broadcaster.instance_id()).await;
    event_cache.append(&event).await;

    Ok(CreateExternalEventOutcome::Created(Box::new(event)))
}

/// `CreateDirectEvent`'s own twin of `create_and_insert_external_event`
/// above - same reasoning, same fix, only the adapter type and the
/// absent `source_content`/`source_context` differ.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all)]
pub async fn create_and_insert_direct_event(
    pool: &Pool,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    adapter: &DirectCreationToken,
    payload: String,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    now: DateTime<Utc>,
    encryption_master_key: Option<&EncryptionMasterKey>,
) -> crate::error::Result<Event> {
    let bounded_context_name = adapter.event_type.bounded_context.name.clone();

    let mut resolved = std::collections::HashMap::new();
    resolve_encryption_keys(
        pool,
        &bounded_context_name,
        &adapter.event_type.sensitive_fields,
        &payload,
        encryption_master_key,
        &mut resolved,
    )
    .await?;

    let mut tx = pool.begin().await?;
    let next_seq = next_sequence(&mut *tx, &bounded_context_name).await?;
    let event = crate::event_store::create_direct_event(
        adapter,
        payload,
        correlation_id,
        causation_id,
        next_seq,
        now,
        |subject_key, subject_value| {
            let (key, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect(
                    "resolve_encryption_keys pre-resolved every subject sensitive_field_subjects \
                     named",
                );
            (key.clone(), data_key.clone())
        },
    )?;
    let encryption_key_ids = encryption_key_ids(&event.encryption_keys, &resolved);
    // Fetched only now - after `next_sequence` above already took this
    // bounded context's own lock - see `insert_event_and_update_sync_projections_in_tx`'s
    // own doc comment on why that ordering, not "as early as possible", is
    // what keeps this read race-free against `promote_projection_rebuild`.
    let sync_projections =
        sync_projections_for_bounded_context(pool, &bounded_context_name).await?;
    insert_event_and_update_sync_projections_in_tx(
        &mut tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
        &sync_projections,
    )
    .await?;
    tx.commit().await?;
    broadcaster.publish(&event);
    record_event_appended(&event);
    notify_event_appended(pool, &event, broadcaster.instance_id()).await;
    event_cache.append(&event).await;

    Ok(event)
}

/// What `submit_command` below settles on - either governs a real insert
/// (`Accepted`, already persisted by the time this returns) or a
/// legitimate business rejection (`Rejected`, nothing persisted at all).
/// `ProcessCommand`'s own two `CommandDecision` outcomes, but carrying
/// the *actual* result - which may differ from the caller's own
/// optimistic `dispatch` call if a DCB conflict forced a retry, see
/// `submit_command`'s own doc comment.
#[derive(Debug)]
pub enum SubmitCommandOutcome {
    Accepted {
        // Boxed for the same reason `EventOrigin::CommandTriggered`'s own
        // `command` field is - `Command` is large enough next to
        // `Rejected`'s two `String`s that clippy's `large_enum_variant`
        // flags it otherwise.
        command: Box<Command>,
        events: Vec<Event>,
    },
    Rejected {
        reason: String,
        kind: String,
        // Codeberg issue #7's DCB conflict visualizer: the tag-scoped
        // event set `decide()` actually evaluated against for *this*
        // rejection - the caller-supplied `matching_events` parameter
        // below for an ordinary rejection, or the freshly-recomputed
        // set from this function's own DCB-conflict redispatch branch
        // if that's what produced it (never the caller's now-stale
        // pre-lock one in that case) - see this function's own doc
        // comment.
        matching_events: Vec<Event>,
    },
    /// A duplicate submission bearing an `idempotency_key` that already
    /// produced a real `Accepted` outcome (Codeberg issue #12) - not a
    /// new decision, the *original* one, verbatim. `decide()` never ran
    /// again; no event was inserted again. Rejected outcomes are never
    /// deduplicated - they have zero side effects, so a duplicate one is
    /// simply re-decided fresh, correctly, every time (see
    /// `submit_command`'s own doc comment for why).
    Deduplicated { triggered_event_sequences: Vec<i64> },
}

/// A `submit_command` call's own snapshot-acceleration context
/// ([docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)) - `Some` when the caller took the
/// snapshot-accelerated path (`CommandDispatcher::dispatch_from_snapshot`)
/// instead of the ordinary one (`dispatch`) for its own `initial_decision`.
/// `state_json` is handed to `dispatch_from_snapshot` again on a
/// DCB-conflict redispatch (made fresh, here, exactly like the ordinary
/// path's own `dispatch` re-call); `as_of_sequence` is the snapshot's
/// own high-water mark - the floor `original_highest` needs when
/// `bounded_context_events` (the caller's own `events_since_snapshot`)
/// is empty, since a plain `.unwrap_or(-1)` there would wrongly treat
/// "nothing changed since the snapshot" as "nothing has ever happened".
///
/// **Known limitation, narrow and audit-only**: `Command.consistency_boundary`
/// can under-report as `None` for a snapshot-accelerated command whose
/// own `events_since_snapshot` is empty, even though the snapshot's own
/// folded prefix represents real prior history -
/// `consistency_boundary_and_matching_events` has no way to know about
/// `as_of_sequence`, only about `bounded_context_events`. This never
/// affects `decide_from_snapshot`'s own inputs or the decision it
/// produces - only that one audit field - so it's left as a documented
/// gap for this pass rather than widening `process_command`'s own
/// signature too.
pub struct SnapshotContext<'a> {
    pub state_json: &'a str,
    pub as_of_sequence: i64,
}

/// The shared "locked half" of `ProcessCommand` - `skilj-rest`'s
/// `post_commands_trigger` and `skilj-graphql`'s `submitCommand` both
/// delegate to this once authorisation is done and `initial_decision` -
/// `dispatch`'s own first, optimistic, unlocked call against
/// `bounded_context_events` - is already in hand. Implements the
/// optimistic-then-locked pattern the note above the rules in
/// specs/skilj.allium describes: "Only the final re-check and insert
/// need the lock; reading matching events and running decide()
/// beforehand does not."
///
/// Opens one transaction and locks `bounded_context`'s own `sequence`
/// row up front - `SELECT ... FOR UPDATE`, peeking its current value
/// rather than `next_sequence`'s own increment-and-return, since how
/// many sequence numbers this submission ends up needing isn't known
/// until the decision that finally governs it is. If that peek shows
/// more has been committed than `bounded_context_events` (the caller's
/// own optimistic read - already tag-scoped to `consistency_tags` by
/// every real call site, [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 1" fix)
/// already reflected, and at least one of those new arrivals matches
/// `consistency_tags`, that is the DCB conflict this lock exists to
/// catch (see `DynamicConsistencyBoundaryHonoured`):
/// `dispatch` is called again, now with a `matching_events` set that
/// includes the new arrival, and the fresh decision it returns supersedes
/// `initial_decision`. At most one such retry is ever needed - once the
/// lock is held, nothing else can commit to this bounded context until
/// this transaction ends (see the note above the rules: "no concurrent
/// committer for the same bounded context can be interleaved while the
/// row is locked"), so there is nothing further this call could miss.
///
/// Whichever decision ends up governing, `event_store::process_command`,
/// `insert_command`, and `insert_event_and_update_sync_projections_in_tx`
/// (once per triggered event) all run inside that same transaction and
/// share its one commit; a rejection (initial or retried) instead leaves
/// `tx` uncommitted - dropped with nothing but the read-only peek lock
/// ever taken, the same implicit-rollback-on-drop every early `?` return
/// elsewhere in this module already relies on. Either way,
/// `SequenceIsGaplessPerBoundedContext` holds for real: any failure
/// anywhere in this function (a rejection, an unregistered event type, a
/// database error) rolls the whole transaction back, sequence allocation
/// included, rather than burning a sequence number on a write that never
/// lands - the actual bug this function exists to close.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        bounded_context = %command_type.bounded_context.name,
        command_type = %command_type.name,
    )
)]
pub async fn submit_command(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    bounded_context_events: &[Event],
    consistency_tags: &[Tag],
    matching_events: &[Event],
    initial_decision: crate::shared::CommandDecision,
    encryption_master_key: Option<&EncryptionMasterKey>,
    now: DateTime<Utc>,
    snapshot: Option<SnapshotContext<'_>>,
    idempotency_key: Option<&str>,
) -> crate::error::Result<SubmitCommandOutcome> {
    let bounded_context_name = command_type.bounded_context.name.clone();

    // Codeberg issue #32: a pre-lock warm-up for the common, no-DCB-conflict
    // path - mirrors `create_and_insert_external_event`/
    // `create_and_insert_direct_event`'s own "resolve encryption keys
    // before opening the transaction" shape, extended here to event-type
    // lookups too. See `warm_up_event_types_and_encryption_keys`'s own
    // doc comment for why this is always safe, never just an
    // optimisation this function's own correctness depends on.
    let (event_types_by_name, resolved) = warm_up_event_types_and_encryption_keys(
        pool,
        &bounded_context_name,
        command_type,
        payload,
        &initial_decision,
        encryption_master_key,
    )
    .await?;

    let mut tx = pool.begin().await?;
    let locked_highest = lock_bounded_context_sequence(&mut tx, &bounded_context_name).await?;
    // Fetched once for the whole submission, not once per event (Codeberg
    // issue #32) - and only now, after the lock above is already held,
    // which is what keeps this read race-free against
    // `promote_projection_rebuild` (see
    // `insert_event_and_update_sync_projections_in_tx`'s own doc comment).
    let sync_projections =
        sync_projections_for_bounded_context(pool, &bounded_context_name).await?;

    let outcome = submit_one_command_in_tx(
        &mut tx,
        pool,
        dispatcher,
        projection_dispatcher,
        command_type,
        payload,
        client_id,
        correlation_id,
        causation_id,
        bounded_context_events,
        consistency_tags,
        matching_events,
        initial_decision,
        encryption_master_key,
        now,
        snapshot,
        idempotency_key,
        locked_highest,
        &sync_projections,
        &[],
        event_types_by_name,
        resolved,
    )
    .await?;

    tx.commit().await?;
    broadcast_appended_events(pool, broadcaster, event_cache, &outcome).await;

    Ok(outcome)
}

/// The pre-lock warm-up `submit_command`/`command_batcher::CommandBatcher::submit`
/// both run before ever opening a transaction: every `EventType` an
/// accepted `initial_decision`'s own event_specs names, and every
/// `EncryptionKey` those events' (and the command's own) sensitive
/// fields need. A no-op (both maps come back empty) for a `Rejected`
/// `initial_decision` - nothing to warm up for a command that was never
/// going to write anything.
///
/// **Always safe, never a correctness dependency**: `submit_one_command_in_tx`'s
/// own post-lock code re-runs the identical resolution unconditionally
/// against whatever `event_specs` actually governs by the time it runs
/// (the redispatched ones, if a DCB conflict forced a retry) - its
/// `Entry::Vacant`/`resolved.contains_key` guards make every call here a
/// no-op the second time, and tolerate this warm-up being partial, stale,
/// or skipped entirely just as well as they tolerate it being complete.
/// This function exists purely to shorten the bounded-context lock's own
/// hold span (`submit_command`) or to move that work fully in parallel,
/// before a request ever joins a shared batch queue at all
/// (`CommandBatcher::submit`) - not to change what's correct.
pub async fn warm_up_event_types_and_encryption_keys(
    pool: &Pool,
    bounded_context_name: &str,
    command_type: &CommandType,
    payload: &str,
    initial_decision: &crate::shared::CommandDecision,
    encryption_master_key: Option<&EncryptionMasterKey>,
) -> crate::error::Result<(
    std::collections::HashMap<String, EventType>,
    std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
)> {
    let mut event_types_by_name: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut resolved = std::collections::HashMap::new();
    if let crate::shared::CommandDecision::Accepted { events } = initial_decision {
        for spec in events {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                event_types_by_name.entry(spec.event_type.clone())
            {
                if let Some(et) =
                    get_event_type(pool, bounded_context_name, &spec.event_type).await?
                {
                    entry.insert(et);
                }
            }
        }
        resolve_encryption_keys(
            pool,
            bounded_context_name,
            &command_type.sensitive_fields,
            payload,
            encryption_master_key,
            &mut resolved,
        )
        .await?;
        for spec in events {
            if let Some(event_type) = event_types_by_name.get(&spec.event_type) {
                let spec_payload = spec.payload.to_string();
                resolve_encryption_keys(
                    pool,
                    bounded_context_name,
                    &event_type.sensitive_fields,
                    &spec_payload,
                    encryption_master_key,
                    &mut resolved,
                )
                .await?;
            }
        }
    }
    Ok((event_types_by_name, resolved))
}

/// The `SELECT ... FOR UPDATE` peek `submit_command`'s own doc comment
/// describes, pulled out so `submit_command_batch`'s leader can take it
/// exactly once per batch too, instead of once per command - the actual
/// mechanism this whole pass (Codeberg issue #32, round two) exists to
/// amortise across as many concurrently-arriving commands as possible.
async fn lock_bounded_context_sequence(
    tx: &mut Transaction<'_, Postgres>,
    bounded_context: &str,
) -> crate::error::Result<i64> {
    let schema = schema_ident(bounded_context);
    let (locked_highest,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT next_value FROM {schema}.sequence FOR UPDATE"
    )))
    .fetch_one(&mut **tx)
    .await?;
    Ok(locked_highest)
}

/// `EventSubscription`'s own real-time delivery - after the commit, not
/// before, the same rule `insert_event_and_update_sync_projections`
/// itself already follows. Shared by `submit_command`'s own single-request
/// path and `command_batcher::CommandBatcher`'s batched one - both commit
/// once, then need every event that commit produced broadcast exactly
/// this way; a no-op for `Rejected`/`Deduplicated` outcomes, which
/// produced nothing to broadcast.
pub async fn broadcast_appended_events(
    pool: &Pool,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    outcome: &SubmitCommandOutcome,
) {
    let SubmitCommandOutcome::Accepted { events, .. } = outcome else {
        return;
    };
    for event in events {
        broadcaster.publish(event);
        record_event_appended(event);
        notify_event_appended(pool, event, broadcaster.instance_id()).await;
        event_cache.append(event).await;
    }
}

/// What [`decide_command_in_tx`] settles on for a command whose decision
/// was `Accepted` - everything [`finish_accepted_command_in_tx`] needs to
/// actually write it, once sequence numbers are available. Kept as its
/// own struct, rather than inlining decide's tail into finish, is what
/// lets `commit_command_batch`'s own loop insert a [`SequencePool`] draw
/// in between the two calls.
struct AcceptedDecision {
    event_specs: Vec<crate::shared::EventSpec>,
    final_bounded_context_events: Vec<Event>,
    event_types_by_name: std::collections::HashMap<String, EventType>,
}

/// [`decide_command_in_tx`]'s own return type - either a terminal outcome
/// (`Rejected`/`Deduplicated`) that never needs a sequence number at all,
/// or an [`AcceptedDecision`] still needing [`finish_accepted_command_in_tx`]
/// to actually persist it.
enum DecideOutcome {
    Terminal(SubmitCommandOutcome),
    Accepted(AcceptedDecision),
}

/// The read-only, no-sequence-number-needed half of what used to be one
/// `submit_one_command_in_tx` - split out (Codeberg issue #32, round
/// three: shrinking a batch's own per-command round trips further, after
/// round two's group commit already amortised the lock acquisition
/// itself) so `commit_command_batch`'s loop can run this directly
/// against the *leader's own* `tx`, not a per-command `SAVEPOINT`. That
/// matters for exactly one reason: it lets a [`SequencePool`] reservation -
/// acquired against that same `tx`, in the gap between this call and
/// [`finish_accepted_command_in_tx`]'s own `SAVEPOINT` - survive a later
/// command's real failure instead of being rolled back with it, which is
/// what makes pooling sequence numbers across several commands safe at
/// all. See `SequencePool`'s own doc comment for the full reasoning.
///
/// Never touches `next_sequence_batch`, `resolve_encryption_keys`,
/// `process_command`, or any insert - everything here is either a pure
/// function or a read (`lookup_idempotency_key` against `tx` itself,
/// everything else against `pool`), so running it directly on a
/// long-lived `tx` shared by many commands, rather than inside its own
/// disposable transaction, changes nothing about what it can safely see
/// or do; a genuine failure here (an idempotency-lookup DB error, a
/// decider error) is returned as a real `Err` and, in `commit_command_batch`'s
/// own caller, is still wrapped in its own tiny `SAVEPOINT` purely so
/// that failure can't sour `tx` for the commands still to come - see
/// that function's own comment.
///
/// `event_types_by_name` arrives already warmed up (see
/// `warm_up_event_types_and_encryption_keys`) and is grown in place for
/// whatever a redispatch still needs - identical to how `submit_command`
/// itself used to do this inline.
#[allow(clippy::too_many_arguments)]
async fn decide_command_in_tx(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    bounded_context_events: &[Event],
    consistency_tags: &[Tag],
    matching_events: &[Event],
    initial_decision: crate::shared::CommandDecision,
    snapshot: Option<SnapshotContext<'_>>,
    idempotency_key: Option<&str>,
    locked_highest: i64,
    extra_committed_events: &[Event],
    extra_committed_idempotency_keys: &std::collections::HashMap<
        (String, String, String),
        Vec<i64>,
    >,
    mut event_types_by_name: std::collections::HashMap<String, EventType>,
) -> crate::error::Result<DecideOutcome> {
    let bounded_context_name = command_type.bounded_context.name.clone();
    let schema = schema_ident(&bounded_context_name);
    let original_highest = bounded_context_events
        .iter()
        .map(|e| e.sequence)
        .max()
        .unwrap_or_else(|| snapshot.as_ref().map(|s| s.as_of_sequence).unwrap_or(-1));

    // Codeberg issue #12: a cached prior answer, not a new decision -
    // checked as early as possible, right after the lock that makes this
    // race-free (see `lookup_idempotency_key`'s own doc comment).
    // `initial_decision`/the redispatch logic below never runs on a hit -
    // nothing is written, and this command never reaches
    // `finish_accepted_command_in_tx` at all.
    //
    // Same two-source shape `extra_committed_events` already uses for the
    // DCB delta below it: `extra_committed_idempotency_keys` is this
    // batch's own in-memory record of idempotency keys an *earlier*
    // command in this same, still-uncommitted batch already claimed
    // (`commit_command_batch`'s own accumulator, populated right after
    // each accepted command's own persist succeeds) - the only case a
    // plain `pool` read below can't see, since those rows live only on
    // the leader's own connection until the whole batch commits. Anything
    // genuinely already committed by an earlier, separate transaction *is*
    // visible to a plain `pool` read, for the identical reason
    // `lookup_idempotency_key`'s own doc comment gives the DCB delta query
    // a few lines below: the bounded-context lock already fully
    // serializes every writer, so nothing uncommitted from any *other*
    // transaction can exist to miss. This is what lets `decide_command_in_tx`
    // run without ever touching the leader's own `tx` at all - no
    // per-command `SAVEPOINT` is needed to isolate a DB error here, since
    // there is no `tx`-scoped statement left to isolate one from.
    if let Some(key) = idempotency_key {
        let cache_key = (
            command_type.name.clone(),
            client_id.to_string(),
            key.to_string(),
        );
        let triggered_event_sequences = match extra_committed_idempotency_keys.get(&cache_key) {
            Some(sequences) => Some(sequences.clone()),
            None => {
                lookup_idempotency_key(pool, &schema, &command_type.name, client_id, key).await?
            }
        };
        if let Some(triggered_event_sequences) = triggered_event_sequences {
            return Ok(DecideOutcome::Terminal(
                SubmitCommandOutcome::Deduplicated {
                    triggered_event_sequences,
                },
            ));
        }
    }

    let mut final_decision = initial_decision;
    let mut final_bounded_context_events = bounded_context_events.to_vec();
    // Codeberg issue #7: starts as the caller's own pre-lock set,
    // overwritten below only if a DCB conflict forced a redispatch -
    // whichever one actually produced `final_decision` is the one a
    // rejection reports (see `SubmitCommandOutcome::Rejected`'s own doc
    // comment).
    let mut final_matching_events = matching_events.to_vec();

    // Something committed between the caller's own optimistic read and
    // this lock - either genuinely committed (`locked_highest >
    // original_highest`, fetched from `pool`) or, new in this pass, an
    // earlier command in this same batch (`extra_committed_events`,
    // already sitting in `tx` but invisible to a `pool` query since it
    // isn't committed yet). Only a match on our own consistency_tags is
    // an actual DCB conflict; see docs/architecture.md §19's "Problem 1"
    // fix for why the `pool` half of this is already tag-indexed rather
    // than an unfiltered range scan.
    let mut delta = if locked_highest > original_highest {
        // `command_type.bounded_context` is already this exact row -
        // every command a `CommandBatcher` batch ever holds shares one
        // bounded context (the queue is keyed on it), so there is never
        // a fresher copy to fetch here. See
        // `list_events_for_bounded_context_matching_tags_with_bc`'s own
        // doc comment for why re-fetching it per command was pure
        // round-trip waste inside the batch leader's held lock.
        //
        // Timed separately (Codeberg issue #32, round four investigation)
        // - `commit_command_batch`'s own per-batch `decide_us` total was
        // far higher than CPU-only decide work should cost; this pins
        // down whether this specific query, firing whenever a busy
        // batch's fixed `locked_highest` has outrun some individual
        // command's own pre-lock snapshot (which real concurrent load
        // makes the common case, not the rare-conflict case this branch
        // was written for), is where that time actually goes.
        let delta_query_started = std::time::Instant::now();
        let result = list_events_for_bounded_context_matching_tags_with_bc(
            pool,
            &command_type.bounded_context,
            consistency_tags,
            Some(original_highest),
            Some(&event_types_by_name),
        )
        .await?;
        tracing::info!(
            bounded_context = %bounded_context_name,
            delta_query_us = delta_query_started.elapsed().as_micros(),
            delta_rows = result.len(),
            "command decide delta query"
        );
        result
    } else {
        Vec::new()
    };
    delta.extend(
        extra_committed_events
            .iter()
            .filter(|e| {
                e.sequence > original_highest && consistency_tags.iter().any(|t| e.tags.contains(t))
            })
            .cloned(),
    );

    if !delta.is_empty() {
        final_bounded_context_events.extend(delta);
        final_bounded_context_events.sort_by_key(|e| e.sequence);
        let (_boundary, redispatch_matching_events) =
            crate::event_store::consistency_boundary_and_matching_events(
                &final_bounded_context_events,
                consistency_tags,
            );
        // docs/architecture.md §19: a snapshot-accelerated initial
        // decision redispatches through `dispatch_from_snapshot`
        // again too, not the ordinary `dispatch` - the snapshot's
        // own folded prefix (`snapshot.state_json`) still represents
        // real history `redispatch_matching_events` alone doesn't
        // (it's the tag-indexed delta since the snapshot, same as
        // `bounded_context_events` already was); calling the
        // ordinary path here would silently drop everything the
        // snapshot had already folded.
        final_decision = match &snapshot {
            Some(ctx) => match dispatcher.dispatch_from_snapshot(
                &bounded_context_name,
                &command_type.name,
                payload,
                ctx.state_json,
                &redispatch_matching_events,
            ) {
                None => return Err(crate::error::Error::NoDeciderRegistered),
                Some(Err(e)) => return Err(e),
                Some(Ok(d)) => d,
            },
            None => match dispatcher.dispatch(
                &bounded_context_name,
                &command_type.name,
                payload,
                &redispatch_matching_events,
            ) {
                None => return Err(crate::error::Error::NoDeciderRegistered),
                Some(Err(e)) => return Err(e),
                Some(Ok(d)) => d,
            },
        };
        final_matching_events = redispatch_matching_events;
    }

    let event_specs = match final_decision {
        crate::shared::CommandDecision::Rejected { reason, kind } => {
            COMMANDS_PROCESSED.add(
                1,
                &[
                    KeyValue::new("bounded_context", command_type.bounded_context.name.clone()),
                    KeyValue::new("command_type", command_type.name.clone()),
                    KeyValue::new("outcome", "rejected"),
                ],
            );
            return Ok(DecideOutcome::Terminal(SubmitCommandOutcome::Rejected {
                reason,
                kind,
                matching_events: final_matching_events,
            }));
        }
        crate::shared::CommandDecision::Accepted { events } => events,
    };

    // process_command's own resolve_event_type stays a plain sync closure
    // (decide() and everything downstream is I/O-free per §1.1) - every
    // EventType lookup this call will need happens against the *final*
    // event_specs (the redispatched ones, if a retry happened above).
    // `event_types_by_name` arrived already warmed up for `initial_decision`'s
    // own specs; this loop is what a redispatch's different specs still
    // need, and its `Entry::Vacant` guard means it costs nothing extra
    // when nothing changed.
    for spec in &event_specs {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            event_types_by_name.entry(spec.event_type.clone())
        {
            if let Some(et) = get_event_type(pool, &bounded_context_name, &spec.event_type).await? {
                entry.insert(et);
            }
        }
    }

    Ok(DecideOutcome::Accepted(AcceptedDecision {
        event_specs,
        final_bounded_context_events,
        event_types_by_name,
    }))
}

/// The write half of what used to be one `submit_one_command_in_tx` -
/// everything [`decide_command_in_tx`] couldn't do without real sequence
/// numbers in hand. `sequences` is exactly `decided.event_specs.len()`
/// numbers, already allocated by the caller (a plain `next_sequence_batch`
/// call for `submit_command`'s own batch-of-one path, a shared
/// [`SequencePool`] draw for `commit_command_batch`'s multi-command one) -
/// this function itself never touches `{schema}.sequence` at all.
#[allow(clippy::too_many_arguments)]
async fn finish_accepted_command_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    pool: &Pool,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    encryption_master_key: Option<&EncryptionMasterKey>,
    now: DateTime<Utc>,
    idempotency_key: Option<&str>,
    sync_projections: &[Projection],
    sequences: Vec<i64>,
    decided: AcceptedDecision,
    mut resolved: std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
) -> crate::error::Result<SubmitCommandOutcome> {
    let bounded_context_name = command_type.bounded_context.name.clone();
    let schema = schema_ident(&bounded_context_name);
    let AcceptedDecision {
        event_specs,
        final_bounded_context_events,
        event_types_by_name,
    } = decided;
    let mut sequences = sequences.into_iter();

    // protect_sensitive_fields' own pre-resolution step, for the
    // command's own payload *and* every final event spec's - see
    // `resolve_encryption_keys`'s own doc comment. Runs against `pool`,
    // not `tx`, deliberately - EncryptionKey provisioning staying
    // outside this transaction is exactly the same "don't hold the
    // sequence row lock across a master-key wrap" reasoning
    // `create_and_insert_external_event`'s own doc comment gives, doubly
    // so here since this is the lock `submit_command`/`CommandBatcher`
    // itself holds. `resolved` arrived already warmed up for
    // `initial_decision`'s own payloads.
    //
    // Every distinct `(subject_key, subject_value)` subject the command's
    // own payload and every accepted event's payload will need is
    // gathered up front instead of resolved payload-by-payload -
    // `sensitive_field_subjects` is pure and I/O-free, so collecting all
    // of them first costs nothing. Deduped here (and against whatever
    // `resolved` already carries) so the concurrent resolution below
    // never double-provisions the same subject twice - the same
    // guarantee `resolve_encryption_keys`'s own serial `contains_key`
    // skip gave one payload at a time, just computed across every
    // payload in this command in one pass instead of only within each
    // call. Once deduped, every remaining subject is provably distinct,
    // so - unlike resolving them one payload at a time - they're safe to
    // resolve concurrently: what used to be N round trips serialized
    // inside this same held lock becomes one concurrent batch of them.
    let mut needed_subjects: Vec<(String, String)> =
        crate::event_store::sensitive_field_subjects(&command_type.sensitive_fields, payload);
    for spec in &event_specs {
        if let Some(event_type) = event_types_by_name.get(&spec.event_type) {
            needed_subjects.extend(crate::event_store::sensitive_field_subjects(
                &event_type.sensitive_fields,
                &spec.payload.to_string(),
            ));
        }
    }
    needed_subjects.retain(|pair| !resolved.contains_key(pair));
    needed_subjects.sort();
    needed_subjects.dedup();

    if !needed_subjects.is_empty() {
        let master_key = encryption_master_key.ok_or(encryption::Error::MasterKeyNotConfigured)?;
        let provisioned = futures_util::future::try_join_all(needed_subjects.iter().map(
            |(subject_key, subject_value)| {
                get_or_create_encryption_key(
                    pool,
                    &bounded_context_name,
                    subject_key,
                    subject_value,
                    master_key,
                )
            },
        ))
        .await?;
        for (subject, provisioned) in needed_subjects.into_iter().zip(provisioned) {
            resolved.insert(subject, provisioned);
        }
    }

    let result = crate::event_store::process_command(
        crate::shared::generate_token_id(),
        command_type,
        payload,
        client_id,
        correlation_id,
        causation_id,
        &final_bounded_context_events,
        crate::shared::CommandDecision::Accepted {
            events: event_specs,
        },
        |name| event_types_by_name.get(name).cloned(),
        || {
            sequences.next().expect(
                "process_command called next_sequence more times than there are accepted events",
            )
        },
        now,
        |subject_key, subject_value| {
            let (key, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect(
                    "resolve_encryption_keys pre-resolved every subject sensitive_field_subjects \
                     named",
                );
            (key.clone(), data_key.clone())
        },
    )?;

    // Command and every one of its triggered events, in the one
    // (savepoint-scoped, in the batched path) transaction `tx` has held
    // since the lock above - DynamicConsistencyBoundaryHonoured's actual
    // enforcement: a failure partway through this loop rolls the command
    // insert back too, rather than leaving a persisted Command with only
    // some of its events.
    let command_key_ids = encryption_key_ids(&result.command.encryption_keys, &resolved);
    let command_id = insert_command(tx, &result.command, &command_key_ids).await?;
    for event in &result.events {
        let event_key_ids = encryption_key_ids(&event.encryption_keys, &resolved);
        insert_event_and_update_sync_projections_in_tx(
            tx,
            event,
            Some(command_id),
            projection_dispatcher,
            &event_key_ids,
            sync_projections,
        )
        .await?;
    }

    if let Some(key) = idempotency_key {
        let triggered_event_sequences: Vec<i64> =
            result.events.iter().map(|e| e.sequence).collect();
        insert_idempotency_key(
            &mut **tx,
            &schema,
            &command_type.name,
            client_id,
            key,
            &triggered_event_sequences,
            now,
        )
        .await?;
    }

    COMMANDS_PROCESSED.add(
        1,
        &[
            KeyValue::new("bounded_context", command_type.bounded_context.name.clone()),
            KeyValue::new("command_type", command_type.name.clone()),
            KeyValue::new("outcome", "accepted"),
        ],
    );

    Ok(SubmitCommandOutcome::Accepted {
        command: Box::new(result.command),
        events: result.events,
    })
}

/// `submit_command`'s own batch-of-one composition of [`decide_command_in_tx`]
/// and [`finish_accepted_command_in_tx`], with a plain per-call
/// `next_sequence_batch` in between. Unlike `commit_command_batch`'s own
/// loop, there is only ever one command here, so there is nothing to
/// pool sequence numbers *across* - both halves run on the same flat
/// `tx` `submit_command` opened, with no per-command `SAVEPOINT` layered
/// on top of it at all, so a real failure in either half rolls back that
/// one, whole transaction, exactly as before this function was split in
/// two.
#[allow(clippy::too_many_arguments)]
async fn submit_one_command_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    bounded_context_events: &[Event],
    consistency_tags: &[Tag],
    matching_events: &[Event],
    initial_decision: crate::shared::CommandDecision,
    encryption_master_key: Option<&EncryptionMasterKey>,
    now: DateTime<Utc>,
    snapshot: Option<SnapshotContext<'_>>,
    idempotency_key: Option<&str>,
    locked_highest: i64,
    sync_projections: &[Projection],
    extra_committed_events: &[Event],
    event_types_by_name: std::collections::HashMap<String, EventType>,
    resolved: std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
) -> crate::error::Result<SubmitCommandOutcome> {
    let bounded_context_name = command_type.bounded_context.name.clone();
    let decided = match decide_command_in_tx(
        pool,
        dispatcher,
        command_type,
        payload,
        client_id,
        bounded_context_events,
        consistency_tags,
        matching_events,
        initial_decision,
        snapshot,
        idempotency_key,
        locked_highest,
        extra_committed_events,
        // This is `submit_command`'s own batch-of-one path - no other
        // command shares `tx`'s lock hold, so there is never a same-batch
        // idempotency key to find in memory; every real hit comes from
        // `lookup_idempotency_key`'s own `pool` read.
        &std::collections::HashMap::new(),
        event_types_by_name,
    )
    .await?
    {
        DecideOutcome::Terminal(outcome) => return Ok(outcome),
        DecideOutcome::Accepted(decided) => decided,
    };

    // Allocated inside `tx`, after the lock above, in one round trip for
    // every event this command decided - `next_sequence_batch`, not
    // `event_specs.len()` separate `next_sequence` calls (Codeberg issue
    // #32). A failure anywhere below (an unregistered event type,
    // encryption resolution, the inserts themselves) rolls the whole
    // allocation back with the rest of this command's own work - the
    // whole transaction, since this path has no per-command `SAVEPOINT`
    // of its own.
    let sequences = next_sequence_batch(
        &mut **tx,
        &bounded_context_name,
        decided.event_specs.len() as i64,
    )
    .await?;

    finish_accepted_command_in_tx(
        tx,
        pool,
        projection_dispatcher,
        command_type,
        payload,
        client_id,
        correlation_id,
        causation_id,
        encryption_master_key,
        now,
        idempotency_key,
        sync_projections,
        sequences,
        decided,
        resolved,
    )
    .await
}

/// The owned equivalent of [`SnapshotContext`] - that type borrows
/// `state_json`, fine for a caller whose own stack frame outlives the
/// call it's passed into, but a [`BatchedCommand`] has to survive being
/// handed to a *different* task (`command_batcher::CommandBatcher`'s
/// batch leader, which may be a different concurrently-running request's
/// own task entirely) - see `BatchedCommand`'s own doc comment.
#[derive(Debug, Clone)]
pub struct OwnedSnapshotContext {
    pub state_json: String,
    pub as_of_sequence: i64,
}

/// One command's worth of everything [`submit_one_command_in_tx`] needs,
/// entirely owned rather than borrowed - what a request becomes once it
/// has to survive being queued for, and processed by, some other task
/// entirely (`command_batcher::CommandBatcher`'s batch leader is
/// whichever concurrently-submitting caller happened to arrive first;
/// every other command in its batch is, from the leader's own stack
/// frame's perspective, a different task's data). The shared collaborators
/// [`submit_command`]'s own signature also takes - `pool`, `dispatcher`,
/// `projection_dispatcher`, `encryption_master_key` - are deliberately
/// *not* part of this struct: within one process they are the same
/// `Arc`/reference for every command a `CommandBatcher` ever processes
/// (all ultimately sourced from the one `Skilj` instance), so the batch
/// leader already has its own copy and simply reuses it for every command
/// in the batch rather than each one carrying a redundant copy.
pub struct BatchedCommand {
    pub command_type: CommandType,
    pub payload: String,
    pub client_id: String,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub bounded_context_events: Vec<Event>,
    pub consistency_tags: Vec<Tag>,
    pub matching_events: Vec<Event>,
    pub initial_decision: crate::shared::CommandDecision,
    pub now: DateTime<Utc>,
    pub snapshot: Option<OwnedSnapshotContext>,
    pub idempotency_key: Option<String>,
    /// Pre-lock-warmed by this command's own original caller, via
    /// `warm_up_event_types_and_encryption_keys`, before it ever joined a
    /// batch queue - see that function's own doc comment for why this is
    /// always safe and never a correctness dependency.
    pub event_types_by_name: std::collections::HashMap<String, EventType>,
    pub resolved: std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
}

/// What one command in a [`submit_command_batch`] call settles on - the
/// same [`SubmitCommandOutcome`] a standalone `submit_command` call would
/// have produced for it, had it run alone; `Err` only for a real failure
/// (an unregistered event type, a database error) that rolled back just
/// this one command's own savepoint, not its batch-mates'.
pub type BatchedCommandResult = crate::error::Result<SubmitCommandOutcome>;

/// What [`begin_command_batch_leader_tx`] hands to [`commit_command_batch`] -
/// see each of their own doc comments for why acquiring the lock and
/// running the batch are split into two functions at all (Codeberg issue
/// #36).
pub struct CommandBatchLeaderTx {
    tx: Transaction<'static, Postgres>,
    bounded_context_name: String,
    locked_highest: i64,
    sync_projections: Vec<Projection>,
}

/// The lock-acquiring half of [`submit_command_batch`], pulled out
/// (Codeberg issue #36) so `command_batcher::CommandBatcher::run_as_leader`
/// can await this - the one part of the batch's own processing whose
/// duration is meant to grow the batch, per this module's own doc comment
/// on the self-tuning design - *before* draining the queue, not after.
/// The previous, unsplit `submit_command_batch` made `run_as_leader` drain
/// first and lock second, which defeated that self-tuning window (a batch
/// almost never grew past whoever raced to become leader) and, worse,
/// meant that if this step alone hung, the caller had no chance to
/// give every already-queued follower a real answer, since draining
/// happened somewhere the caller no longer controlled once this
/// combined function was already inside it.
///
/// `idle_in_transaction_session_timeout`, when `Some`, is issued as a
/// `SET LOCAL` on this transaction's own connection right after opening
/// it, before the lock wait - a defense-in-depth backstop (Codeberg issue
/// #36's own recommendation #3): if this transaction's connection ever
/// does stall indefinitely - a stuck decider, a hung downstream await,
/// anything that leaves it holding the bounded-context lock without
/// making forward progress - Postgres itself kills the idle session
/// after the timeout, releasing the lock so a later batch can proceed
/// instead of every writer to this bounded context queueing forever
/// behind a wedge no automatic mechanism ever clears. `SET LOCAL` scopes
/// the change to this one transaction; it's gone the moment `tx` commits,
/// rolls back, or (if the timeout itself fires) Postgres closes it, so it
/// never leaks onto whatever this pooled connection is handed to next.
///
/// What the timeout does and doesn't measure: Postgres's idle clock only
/// runs *between* statements on this session, and restarts after each
/// one - so it bounds the gap between two consecutive statements (one
/// command's in-memory decide, or a run of consecutive rejected commands
/// that issue none), never the whole batch. The blocked
/// `SELECT ... FOR UPDATE` lock wait is an *active* statement, so it is
/// not covered (nor meant to be): the timeout only protects the phase
/// after the lock is held.
pub async fn begin_command_batch_leader_tx(
    pool: &Pool,
    bounded_context_name: &str,
    idle_in_transaction_session_timeout: Option<std::time::Duration>,
) -> crate::error::Result<CommandBatchLeaderTx> {
    let mut tx = pool.begin().await?;
    if let Some(timeout) = idle_in_transaction_session_timeout {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET LOCAL idle_in_transaction_session_timeout = '{}ms'",
            timeout.as_millis()
        )))
        .execute(&mut *tx)
        .await?;
    }
    let locked_highest = lock_bounded_context_sequence(&mut tx, bounded_context_name).await?;
    let sync_projections = sync_projections_for_bounded_context(pool, bounded_context_name).await?;
    Ok(CommandBatchLeaderTx {
        tx,
        bounded_context_name: bounded_context_name.to_string(),
        locked_highest,
        sync_projections,
    })
}

/// A prefetched, batch-local block of not-yet-consumed sequence numbers -
/// `commit_command_batch`'s own answer to `next_sequence_batch` being
/// called once per command instead of once per batch (Codeberg issue #32,
/// round three). Reserves numbers in chunks against the *leader's own*
/// `tx`, never a per-command `SAVEPOINT` - specifically so a reservation
/// survives a later command's real failure instead of being rolled back
/// with it: today's per-command `next_sequence_batch` call is what
/// currently makes gapless sequencing safe for a command that fails
/// partway through (its own `SAVEPOINT` rollback un-reserves exactly what
/// it asked for); pooling numbers across several commands means a
/// command can end up holding numbers it never uses (a *later* command in
/// the same batch is the one that fails, or the pool simply over-reserved
/// as a heuristic), and those must still never be lost or duplicated.
///
/// `SequenceIsGaplessPerBoundedContext` stays honoured by two rules,
/// together: (1) a draw's numbers are only ever permanently discarded
/// ([`Self::confirm_last_draw`]) once the command that used them has
/// actually committed; (2) a draw that instead fails
/// ([`Self::return_last_draw`]) goes back onto the *front* of the queue,
/// in its original order, so it is the very next thing handed to
/// whichever command asks next - recycled, never wasted mid-batch. Only
/// genuinely unused numbers - ones nobody ever drew, or ones returned and
/// never redrawn because the batch ended first - are corrected back out
/// of `{schema}.sequence` in one shot ([`Self::shrink_back`]), right
/// before `tx.commit()`. Because every draw is either confirmed or
/// returned before the next one starts (this pool is only ever driven by
/// `commit_command_batch`'s own strictly sequential loop, never
/// concurrently), the persisted `next_value` this leaves behind is always
/// identical to what calling `next_sequence_batch` once per command,
/// inside each one's own `SAVEPOINT`, would have left - only the number
/// of round trips to get there differs.
struct SequencePool {
    reserved: std::collections::VecDeque<i64>,
    last_draw: Vec<i64>,
}

impl SequencePool {
    fn new() -> Self {
        Self {
            reserved: std::collections::VecDeque::new(),
            last_draw: Vec::new(),
        }
    }

    /// Pops `count` sequence numbers, refilling from `tx` first if the
    /// pool doesn't already have enough. `refill_hint` (`commit_command_batch`
    /// passes "how many commands are still left in this batch, this one
    /// included") sizes that refill generously so a run of small,
    /// single-event commands shares one round trip instead of paying for
    /// one each; `refill_hint.max(count)` guarantees the refill is always
    /// big enough for the command that triggered it, regardless of how
    /// small a hint the caller passed. Remembers exactly what it handed
    /// out as `last_draw`, so the caller can later call exactly one of
    /// [`Self::confirm_last_draw`] or [`Self::return_last_draw`] once
    /// that command's own outcome is known - never both, never neither.
    async fn take(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
        bounded_context: &str,
        count: i64,
        refill_hint: i64,
    ) -> crate::error::Result<Vec<i64>> {
        if (self.reserved.len() as i64) < count {
            let refill =
                next_sequence_batch(&mut **tx, bounded_context, refill_hint.max(count)).await?;
            self.reserved.extend(refill);
        }
        let drawn: Vec<i64> = (0..count)
            .map(|_| {
                self.reserved
                    .pop_front()
                    .expect("just topped up the pool to at least `count`")
            })
            .collect();
        self.last_draw = drawn.clone();
        Ok(drawn)
    }

    /// The command that drew `last_draw` committed successfully - those
    /// numbers are truly spent, never returned to the pool.
    fn confirm_last_draw(&mut self) {
        self.last_draw.clear();
    }

    /// The command that drew `last_draw` failed - those numbers were
    /// never actually used by any inserted event, so they go back onto
    /// the *front* of the queue (preserving their original order) for
    /// whichever command draws next, instead of being permanently lost.
    fn return_last_draw(&mut self) {
        for value in self.last_draw.drain(..).rev() {
            self.reserved.push_front(value);
        }
    }

    /// Gives back whatever's left reserved-but-never-confirmed once the
    /// batch's own loop is done - see this struct's own doc comment for
    /// why this one corrective `UPDATE`, run once per batch, is all
    /// `SequenceIsGaplessPerBoundedContext` needs, regardless of how many
    /// refills happened or how many draws were returned and re-drawn
    /// along the way.
    async fn shrink_back(
        self,
        tx: &mut Transaction<'_, Postgres>,
        bounded_context: &str,
    ) -> crate::error::Result<()> {
        debug_assert!(
            self.last_draw.is_empty(),
            "every draw must be confirmed or returned before shrink_back runs"
        );
        let leftover = self.reserved.len() as i64;
        if leftover > 0 {
            let schema = schema_ident(bounded_context);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.sequence SET next_value = next_value - $1"
            )))
            .bind(leftover)
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }
}

/// The batched form of the "locked half" of `ProcessCommand` -
/// `submit_command`'s own doc comment describes the single-command
/// shape this generalises. Takes the already-locked transaction
/// [`begin_command_batch_leader_tx`] produced and runs every command in
/// `batch`, *in order*, each inside its own nested transaction
/// (`tx.begin()`, which sqlx backs with a real Postgres `SAVEPOINT` for a
/// `Transaction` that is itself already inside one) - so one command's
/// real failure (as opposed to an ordinary `Rejected` decision, which is
/// never an `Err` at all) rolls back only its own work, `ROLLBACK TO
/// SAVEPOINT`, not the whole batch's. Every command that succeeds is
/// `RELEASE SAVEPOINT`d immediately, keeping its writes visible to
/// whichever command in the batch runs next (see `extra_committed_events`
/// below) without making them durable yet - that's still `tx.commit()`,
/// called exactly once, after the whole batch has been processed.
///
/// This is what makes batching commands from *different*, concurrently-
/// submitting callers into one lock acquisition possible at all
/// (Codeberg issue #32, round two - see `command_batcher`'s own module
/// doc comment for the throughput problem this exists to close): the
/// lock `leader_tx` already holds is held for the combined work of every
/// command in `batch`, not re-acquired per command, so N commands that
/// would previously have queued for N separate lock acquisitions instead
/// share one.
///
/// `extra_committed_events` threading: `submit_one_command_in_tx`'s own
/// DCB-conflict redispatch check needs to see events *this same batch*
/// already produced, not just ones truly committed by some earlier,
/// separate transaction - a plain `pool` query can't see them (they're
/// uncommitted, on `tx`'s own connection, invisible to any other
/// connection until `tx.commit()`), so this loop accumulates every
/// accepted command's own `events` in memory as it goes and hands the
/// running total to each subsequent command.
///
/// Returns one [`BatchedCommandResult`] per input command, same order,
/// only once `tx.commit()` has actually succeeded - nothing here is
/// reported back to any caller as durable before it truly is. A failure
/// committing at the end is returned as the outer `Err` instead - at that
/// point no per-command outcome is trustworthy (a failed commit could
/// mean anything committed or nothing did), so the caller
/// (`command_batcher::CommandBatcher`) is expected to treat every command
/// in the batch as failed identically, not to guess from whatever this
/// function got partway through computing.
#[tracing::instrument(skip_all, fields(batch_size = batch.len()))]
pub async fn commit_command_batch(
    leader_tx: CommandBatchLeaderTx,
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    encryption_master_key: Option<&EncryptionMasterKey>,
    batch: Vec<BatchedCommand>,
) -> crate::error::Result<Vec<BatchedCommandResult>> {
    let CommandBatchLeaderTx {
        mut tx,
        bounded_context_name,
        locked_highest,
        sync_projections,
    } = leader_tx;

    COMMAND_BATCH_SIZE.record(
        batch.len() as u64,
        &[KeyValue::new(
            "bounded_context",
            bounded_context_name.clone(),
        )],
    );
    // A plain log line alongside the histogram above - purely so a load
    // test can grep achieved batch sizes straight out of server logs
    // without standing up an OTLP receiver, per
    // docs/load-test-report-2026-09-18*.md's own "skipped OTel/Grafana"
    // reasoning. The histogram is the lasting instrument; this is the
    // zero-infra way to read it for one investigation.
    tracing::debug!(
        bounded_context = %bounded_context_name,
        batch_size = batch.len(),
        "command batch committed"
    );

    // Per-phase wall-clock accumulators (Codeberg issue #32,
    // round four investigation) - the batch-size histogram above already
    // ruled out "batches aren't forming large enough to amortise" as the
    // explanation for the flat ~27/s ceiling; this is the next
    // measurement, not more reasoning from round-trip counting alone:
    // which of decide/sequence-draw/persist actually eats the wall-clock
    // time inside the held lock. One log line per batch, greppable, same
    // zero-infra reasoning as the batch-size line above.
    let mut decide_total = std::time::Duration::ZERO;
    let mut sequence_total = std::time::Duration::ZERO;
    let mut persist_total = std::time::Duration::ZERO;

    let mut results = Vec::with_capacity(batch.len());
    let mut extra_committed_events: Vec<Event> = Vec::new();
    // `decide_command_in_tx`'s own second in-memory accumulator, alongside
    // `extra_committed_events` above - an earlier command in this same,
    // still-uncommitted batch that claimed an idempotency key is only
    // visible here, not to a `pool` read, until the whole batch commits.
    // See `decide_command_in_tx`'s own doc comment for the full reasoning
    // (mirrors `extra_committed_events` exactly). Keyed by
    // `(command_type_name, client_id, idempotency_key)`.
    let mut batch_idempotency_keys: std::collections::HashMap<(String, String, String), Vec<i64>> =
        std::collections::HashMap::new();
    let mut sequence_pool = SequencePool::new();
    let batch_len = batch.len();

    for (idx, item) in batch.into_iter().enumerate() {
        // `decide_command_in_tx` never touches `tx` at all - every read it
        // does runs against `pool` or this loop's own in-memory
        // accumulators (see its own doc comment) - so, unlike the sequence
        // draw and `finish_accepted_command_in_tx` below, it needs no
        // `SAVEPOINT` of its own: there is no `tx`-scoped statement here
        // for one to isolate a DB error from.
        let decide_started = std::time::Instant::now();
        let decide_result = decide_command_in_tx(
            pool,
            dispatcher,
            &item.command_type,
            &item.payload,
            &item.client_id,
            &item.bounded_context_events,
            &item.consistency_tags,
            &item.matching_events,
            item.initial_decision,
            item.snapshot.as_ref().map(|s| SnapshotContext {
                state_json: &s.state_json,
                as_of_sequence: s.as_of_sequence,
            }),
            item.idempotency_key.as_deref(),
            locked_highest,
            &extra_committed_events,
            &batch_idempotency_keys,
            item.event_types_by_name,
        )
        .await;
        decide_total += decide_started.elapsed();

        let decided = match decide_result {
            Ok(DecideOutcome::Terminal(outcome)) => {
                results.push(Ok(outcome));
                continue;
            }
            Ok(DecideOutcome::Accepted(decided)) => decided,
            Err(e) => {
                results.push(Err(e));
                continue;
            }
        };

        // Reserved against `tx` itself, not the `nested` `SAVEPOINT`
        // opened below - see `SequencePool`'s own doc comment for why
        // that's what lets a later command's failure recycle these
        // instead of burning a permanent gap. `refill_hint` is "how many
        // commands (including this one) are still left to process" - an
        // upper bound on how many single-event commands could still
        // share whatever this refill provisions.
        let refill_hint = (batch_len - idx) as i64;
        let sequence_started = std::time::Instant::now();
        let sequence_result = sequence_pool
            .take(
                &mut tx,
                &bounded_context_name,
                decided.event_specs.len() as i64,
                refill_hint,
            )
            .await;
        sequence_total += sequence_started.elapsed();
        let sequences = match sequence_result {
            Ok(sequences) => sequences,
            Err(e) => {
                // A failure bumping `{schema}.sequence` on `tx` itself -
                // not a per-command problem any `SAVEPOINT` isolates,
                // since this call deliberately runs outside one. Same
                // treatment as a final `tx.commit()` failure below:
                // nothing in this batch is trustworthy from here, so
                // every command - this one and everything still
                // queued - gets the caller's own uniform `BatchFailed`
                // treatment instead of a partial `results`.
                return Err(e);
            }
        };

        // Captured before `finish_accepted_command_in_tx` below moves
        // `item.resolved` out of `item` - needed afterward, once the
        // command's own outcome is known, to record its idempotency key
        // (if any) into `batch_idempotency_keys` for the next command's
        // own `decide_command_in_tx` to see.
        let command_type_name = item.command_type.name.clone();
        let client_id = item.client_id.clone();
        let idempotency_key = item.idempotency_key.clone();

        let persist_started = std::time::Instant::now();
        let mut nested = tx.begin().await?;
        let outcome = finish_accepted_command_in_tx(
            &mut nested,
            pool,
            projection_dispatcher,
            &item.command_type,
            &item.payload,
            &item.client_id,
            item.correlation_id.as_deref(),
            item.causation_id.as_deref(),
            encryption_master_key,
            item.now,
            item.idempotency_key.as_deref(),
            &sync_projections,
            sequences,
            decided,
            item.resolved,
        )
        .await;

        match outcome {
            Ok(outcome) => {
                // A failure of this savepoint statement itself (as with
                // `tx.begin()`/`nested.rollback()` below) means the shared
                // connection is unusable, not that this one command is bad,
                // so it deliberately aborts the whole batch: the
                // transaction can no longer be committed, and the earlier
                // commands' writes live only in it. Per-command failures
                // (`finish_accepted_command_in_tx` returning `Err`) are
                // the ones isolated by the savepoint - see the `Err` arm.
                //
                // Releases this command's own savepoint - its writes stay
                // in `tx`, visible to every subsequent command in this
                // same batch, but still no more durable than the rest of
                // `tx` until the one `tx.commit()` below succeeds.
                nested.commit().await?;
                persist_total += persist_started.elapsed();
                sequence_pool.confirm_last_draw();
                if let SubmitCommandOutcome::Accepted { ref events, .. } = outcome {
                    extra_committed_events.extend(events.iter().cloned());
                    if let Some(key) = idempotency_key {
                        let triggered_event_sequences: Vec<i64> =
                            events.iter().map(|e| e.sequence).collect();
                        batch_idempotency_keys.insert(
                            (command_type_name, client_id, key),
                            triggered_event_sequences,
                        );
                    }
                }
                results.push(Ok(outcome));
            }
            Err(e) => {
                // `ROLLBACK TO SAVEPOINT`, awaited explicitly rather than
                // left to `nested`'s own `Drop` - both end up issuing the
                // same statement, but every subsequent command in this
                // loop shares `tx`'s one underlying connection, so the
                // rollback must be known-complete before the next
                // `tx.begin()` reuses it, not merely queued by a
                // fire-and-forget `Drop`.
                nested.rollback().await?;
                persist_total += persist_started.elapsed();
                sequence_pool.return_last_draw();
                results.push(Err(e));
            }
        }
    }

    sequence_pool
        .shrink_back(&mut tx, &bounded_context_name)
        .await?;
    let commit_started = std::time::Instant::now();
    tx.commit().await?;
    let commit_elapsed = commit_started.elapsed();

    tracing::debug!(
        bounded_context = %bounded_context_name,
        batch_size = batch_len,
        decide_us = decide_total.as_micros(),
        sequence_us = sequence_total.as_micros(),
        persist_us = persist_total.as_micros(),
        commit_us = commit_elapsed.as_micros(),
        "command batch phase timing"
    );

    Ok(results)
}

/// [`begin_command_batch_leader_tx`] immediately followed by
/// [`commit_command_batch`], with no `idle_in_transaction_session_timeout`
/// and nothing drained from a shared queue in between - the whole-batch-
/// in-one-call shape every direct caller (this crate's own
/// `submit_command.rs` tests, which hand-build a batch up front) wants.
/// `command_batcher::CommandBatcher::run_as_leader` calls the two halves
/// separately instead, draining its own queue in the gap between them -
/// see that function's own doc comment for why.
pub async fn submit_command_batch(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    encryption_master_key: Option<&EncryptionMasterKey>,
    bounded_context_name: &str,
    batch: Vec<BatchedCommand>,
) -> crate::error::Result<Vec<BatchedCommandResult>> {
    let leader_tx = begin_command_batch_leader_tx(pool, bounded_context_name, None).await?;
    commit_command_batch(
        leader_tx,
        pool,
        dispatcher,
        projection_dispatcher,
        encryption_master_key,
        batch,
    )
    .await
}

/// What [`resolve_command_submission`] settles on - everything both
/// [`decide_and_submit_command`] and `command_batcher::CommandBatcher::decide_and_submit`
/// need to hand off to their own "locked half" ([`submit_command`] or
/// [`CommandBatcher::submit`](crate::command_batcher::CommandBatcher::submit)
/// respectively), computed identically by both.
pub struct ResolvedCommandSubmission {
    pub bounded_context_events: Vec<Event>,
    pub consistency_tags: Vec<Tag>,
    pub matching_events: Vec<Event>,
    pub decision: crate::shared::CommandDecision,
    pub snapshot_context: Option<ResolvedSnapshot>,
}

/// The optimistic, unlocked half of `ProcessCommand`'s own "optimistic
/// decide, then locked submit" sequence - derive consistency tags,
/// resolve a snapshot context if one applies (`resolve_snapshot_context`),
/// fetch matching events (tag-indexed - [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s
/// own "Problem 1" fix), `dispatch()` once optimistically. Shared by
/// [`decide_and_submit_command`] (hands the result to [`submit_command`])
/// and `command_batcher::CommandBatcher::decide_and_submit` (hands it to
/// [`CommandBatcher::submit`](crate::command_batcher::CommandBatcher::submit)
/// instead) - both need the identical resolution, only what happens with
/// it afterward differs.
pub async fn resolve_command_submission(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    event_cache: &crate::event_cache::EventCache,
    command_type: &CommandType,
    payload: &str,
) -> crate::error::Result<ResolvedCommandSubmission> {
    let bounded_context_name = command_type.bounded_context.name.clone();
    let consistency_tags = crate::event_store::derive_tags(&command_type.tag_mappings, payload);

    let snapshot_context = match dispatcher.snapshot_name(&bounded_context_name, &command_type.name)
    {
        Some(Some(snapshot_name)) => {
            resolve_snapshot_context(
                pool,
                &bounded_context_name,
                snapshot_dispatcher,
                snapshot_name,
                &consistency_tags,
            )
            .await?
        }
        _ => None,
    };

    let bounded_context_events = list_events_for_bounded_context_matching_tags_cached(
        pool,
        event_cache,
        &bounded_context_name,
        &consistency_tags,
        snapshot_context.as_ref().map(|ctx| ctx.as_of_sequence),
    )
    .await?;
    let (_boundary, matching_events) = crate::event_store::consistency_boundary_and_matching_events(
        &bounded_context_events,
        &consistency_tags,
    );

    let decision = match &snapshot_context {
        Some(ctx) => dispatcher
            .dispatch_from_snapshot(
                &bounded_context_name,
                &command_type.name,
                payload,
                &ctx.state_json,
                &matching_events,
            )
            .ok_or(crate::error::Error::NoDeciderRegistered)??,
        None => dispatcher
            .dispatch(
                &bounded_context_name,
                &command_type.name,
                payload,
                &matching_events,
            )
            .ok_or(crate::error::Error::NoDeciderRegistered)??,
    };

    Ok(ResolvedCommandSubmission {
        bounded_context_events,
        consistency_tags,
        matching_events,
        decision,
        snapshot_context,
    })
}

/// The full "optimistic decide, then locked submit" sequence
/// `ProcessCommand` describes end to end, for a caller that already has
/// a resolved `CommandType` and a JSON payload in hand - see
/// [`resolve_command_submission`] for the optimistic half this hands off
/// to [`submit_command`] for the real, locked recheck-and-retry.
///
/// Previously this exact sequence was independently duplicated by
/// `skilj-rest`'s `post_commands_trigger` and `skilj-graphql`'s
/// `submitCommand` resolver (each one's own comments cross-referenced
/// the other as "the identical branch") - both now call through here
/// instead, and it is also what `SkiljBuilder`'s cross-context event
/// router (docs/architecture.md's own write-up of that pass) uses to
/// submit a routed command in-process, a third caller with no REST/
/// GraphQL wire concerns of its own to keep separate from this. `None`
/// from `dispatch`/`dispatch_from_snapshot` (no decider registered for
/// this `(bounded_context, command_type)` pair - `CommandDispatcher::
/// dispatch`'s own doc comment on why that's reachable in principle)
/// surfaces as `Error::NoDeciderRegistered`, the same variant every
/// caller already converts into its own wire error today.
///
/// **Not routed through `command_batcher::CommandBatcher`** - this stays
/// the direct, unbatched path, still exactly what it always was. High-
/// volume, externally-triggered submission (REST/GraphQL command
/// submission, parked-delivery redrive) uses `CommandBatcher::decide_and_submit`
/// instead (Codeberg issue #32, round two); the lower-volume, periodic
/// internal ones (the cross-context event router's own tick,
/// `fire_due_deadlines`) still call through here, deliberately - batching
/// buys the least where a caller is already ticking on its own schedule
/// rather than arriving as a burst of concurrent external requests.
#[allow(clippy::too_many_arguments)]
pub async fn decide_and_submit_command(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    encryption_master_key: Option<&EncryptionMasterKey>,
    now: DateTime<Utc>,
    idempotency_key: Option<&str>,
) -> crate::error::Result<SubmitCommandOutcome> {
    let resolved = resolve_command_submission(
        pool,
        dispatcher,
        snapshot_dispatcher,
        event_cache,
        command_type,
        payload,
    )
    .await?;

    submit_command(
        pool,
        dispatcher,
        projection_dispatcher,
        broadcaster,
        event_cache,
        command_type,
        payload,
        client_id,
        correlation_id,
        causation_id,
        &resolved.bounded_context_events,
        &resolved.consistency_tags,
        &resolved.matching_events,
        resolved.decision,
        encryption_master_key,
        now,
        resolved
            .snapshot_context
            .as_ref()
            .map(|ctx| SnapshotContext {
                state_json: &ctx.state_json,
                as_of_sequence: ctx.as_of_sequence,
            }),
        idempotency_key,
    )
    .await
}

/// `cross_context_route_cursors`'s own read - `None` when no row exists
/// at all, which means this route has never had a single catch-up tick
/// run for it since it was registered (a row, once written, is never
/// deleted - see `update_cross_context_route_cursor`). Distinct from a
/// row genuinely holding `-1` (a `CrossContextRouteStartFrom::Latest`
/// route whose very first tick found no `Source` occurrences at all yet
/// to seed past - see `catch_up_cross_context_route`'s own doc comment):
/// that case has already had its one-time seeding tick, so it must never
/// be seeded again, which is exactly why this returns `Option<i64>`
/// rather than collapsing both into the same `-1` sentinel
/// `sequence`/`caught_up_to` use elsewhere.
async fn get_cross_context_route_cursor(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(source_bounded_context);
    let row: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT last_dispatched_sequence FROM {schema}.cross_context_route_cursors \
         WHERE route_name = $1"
    )))
    .bind(route_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(seq,)| seq))
}

/// Also unconditionally clears any retry backoff state (Codeberg issue
/// #21) - every call site advances the cursor past an occurrence that is
/// now *done* one way or another (skipped, rejected, accepted, or
/// parked after retries were exhausted), so whatever retry state applied
/// to it is stale for the next occurrence regardless of which of those
/// outcomes this one was. See `catch_up_cross_context_route`'s own doc
/// comment.
async fn update_cross_context_route_cursor(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
    sequence: i64,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(source_bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.cross_context_route_cursors \
         (route_name, last_dispatched_sequence, updated_at) VALUES ($1, $2, $3) \
         ON CONFLICT (route_name) DO UPDATE SET \
         last_dispatched_sequence = EXCLUDED.last_dispatched_sequence, \
         updated_at = EXCLUDED.updated_at, \
         retry_attempt_count = 0, \
         retry_first_failed_at = NULL, \
         retry_next_attempt_at = NULL"
    )))
    .bind(route_name)
    .bind(sequence)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// `(retry_attempt_count, retry_first_failed_at, retry_next_attempt_at)` -
/// `get_cross_context_route_retry_state`'s own return shape, named only
/// to keep that signature (and clippy) happy, not used anywhere else.
type CrossContextRouteRetryState = (i32, Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// `catch_up_cross_context_route`'s own read of the retry backoff state
/// `record_cross_context_route_retry_failure` writes - `(0, None, None)`
/// when no row exists yet (this route has never ticked, or every prior
/// occurrence succeeded outright), the same "no row = nothing recorded"
/// convention `get_cross_context_route_cursor` already uses one level up.
async fn get_cross_context_route_retry_state(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
) -> crate::error::Result<CrossContextRouteRetryState> {
    let schema = schema_ident(source_bounded_context);
    let row: Option<CrossContextRouteRetryState> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT retry_attempt_count, retry_first_failed_at, retry_next_attempt_at \
             FROM {schema}.cross_context_route_cursors WHERE route_name = $1"
    )))
    .bind(route_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or((0, None, None)))
}

/// Persists one more failed attempt at the route's own blocked
/// head-of-line occurrence - `current_cursor` is `last_dispatched_sequence`'s
/// own current value (unchanged by this call; only present so the
/// `INSERT` branch of `ON CONFLICT` has a value to write for a route
/// whose very first-ever tick is already failing, before
/// `update_cross_context_route_cursor` has ever run for it). Doesn't
/// touch `updated_at` on conflict - that column tracks the cursor's own
/// last *advance*, not the last retry attempt.
#[allow(clippy::too_many_arguments)]
async fn record_cross_context_route_retry_failure(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
    current_cursor: i64,
    attempt_count: i32,
    first_failed_at: DateTime<Utc>,
    next_attempt_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(source_bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.cross_context_route_cursors \
         (route_name, last_dispatched_sequence, updated_at, retry_attempt_count, \
          retry_first_failed_at, retry_next_attempt_at) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (route_name) DO UPDATE SET \
         retry_attempt_count = EXCLUDED.retry_attempt_count, \
         retry_first_failed_at = EXCLUDED.retry_first_failed_at, \
         retry_next_attempt_at = EXCLUDED.retry_next_attempt_at"
    )))
    .bind(route_name)
    .bind(current_cursor)
    .bind(now)
    .bind(attempt_count)
    .bind(first_failed_at)
    .bind(next_attempt_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Codeberg issue #21 - which family of thing a [`ParkedDelivery`]
/// originally was, and therefore how `retryParkedDelivery` redrives it.
/// `CrossContextRoute`'s own `target_bounded_context`/`target_command_type`
/// columns are populated only for this variant; `ExternalEvent`/
/// `CommandTrigger` populate `access_token_id` instead (see
/// `ParkedDelivery`'s own doc comment for the full column-by-kind story).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkedDeliveryKind {
    CrossContextRoute,
    ExternalEvent,
    CommandTrigger,
}

impl ParkedDeliveryKind {
    fn as_str(self) -> &'static str {
        match self {
            ParkedDeliveryKind::CrossContextRoute => "cross_context_route",
            ParkedDeliveryKind::ExternalEvent => "external_event",
            ParkedDeliveryKind::CommandTrigger => "command_trigger",
        }
    }
}

fn parked_delivery_kind_from_str(s: &str) -> ParkedDeliveryKind {
    match s {
        "external_event" => ParkedDeliveryKind::ExternalEvent,
        "command_trigger" => ParkedDeliveryKind::CommandTrigger,
        _ => ParkedDeliveryKind::CrossContextRoute,
    }
}

/// Codeberg issue #21's own generic "parked delivery" record - see
/// `docs/architecture.md`'s parked-deliveries section for the full
/// design. `request_json` always carries enough to redrive a retry on
/// its own:
/// - `CrossContextRoute`: the `Target` command's own already-translated
///   JSON payload (`target_bounded_context`/`target_command_type` name
///   where to submit it - `access_token_id` is `None`).
/// - `ExternalEvent`/`CommandTrigger`: the bridge's own original
///   `ExternalEventRequest`/`CommandTriggerRequest` body, verbatim
///   (`target_bounded_context`/`target_command_type` are `None`;
///   `access_token_id` names which `ExternalEventToken`/`CommandToken`
///   to re-resolve and redrive through - the bridge's own credential,
///   never the token's secret, which is never stored here).
///
/// Lives in the same bounded-context schema the failing delivery itself
/// targeted - for `ExternalEvent`/`CommandTrigger`, that's
/// `access_token_id`'s own token's bounded context, resolved once at
/// ingestion time by `skilj-rest`'s `POST /v1/parked-deliveries` handler
/// (the same capability-based resolution every other REST route already
/// does), not a caller-supplied path/argument.
#[derive(Debug, Clone, PartialEq)]
pub struct ParkedDelivery {
    pub id: String,
    pub source: String,
    pub kind: ParkedDeliveryKind,
    pub identifier: String,
    pub access_token_id: Option<String>,
    pub target_bounded_context: Option<String>,
    pub target_command_type: Option<String>,
    pub request_json: serde_json::Value,
    pub error: String,
    pub attempt_count: i32,
    pub first_failed_at: DateTime<Utc>,
    pub last_failed_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ParkedDeliveryRow {
    id: String,
    source: String,
    kind: String,
    identifier: String,
    access_token_id: Option<String>,
    target_bounded_context: Option<String>,
    target_command_type: Option<String>,
    request_json: Json<serde_json::Value>,
    error: String,
    attempt_count: i32,
    first_failed_at: DateTime<Utc>,
    last_failed_at: DateTime<Utc>,
}

impl From<ParkedDeliveryRow> for ParkedDelivery {
    fn from(row: ParkedDeliveryRow) -> Self {
        ParkedDelivery {
            id: row.id,
            source: row.source,
            kind: parked_delivery_kind_from_str(&row.kind),
            identifier: row.identifier,
            access_token_id: row.access_token_id,
            target_bounded_context: row.target_bounded_context,
            target_command_type: row.target_command_type,
            request_json: row.request_json.0,
            error: row.error,
            attempt_count: row.attempt_count,
            first_failed_at: row.first_failed_at,
            last_failed_at: row.last_failed_at,
        }
    }
}

const PARKED_DELIVERY_COLUMNS: &str = "id, source, kind, identifier, access_token_id, \
    target_bounded_context, target_command_type, request_json, error, attempt_count, \
    first_failed_at, last_failed_at";

/// Codeberg issue #21 - `parked_deliveries`, one row per persistently-
/// failed delivery. A brand-new table, so (unlike
/// `cross_context_route_cursors`'s own retry columns above) no
/// already-provisioned bounded context needs a separate `ALTER TABLE`
/// patch - `CREATE TABLE IF NOT EXISTS`, called from both
/// `provision_bounded_context_schema` and every `build()`'s startup
/// loop, is the whole story, the same "brand-new table needs no
/// migration dance" register `ensure_external_message_cursors_table`'s
/// own doc comment already uses.
#[tracing::instrument(skip_all)]
pub async fn ensure_parked_deliveries_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.parked_deliveries (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            kind TEXT NOT NULL,
            identifier TEXT NOT NULL,
            access_token_id TEXT,
            target_bounded_context TEXT,
            target_command_type TEXT,
            request_json JSONB NOT NULL,
            error TEXT NOT NULL,
            attempt_count INT NOT NULL,
            first_failed_at TIMESTAMPTZ NOT NULL,
            last_failed_at TIMESTAMPTZ NOT NULL
        )"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

const PARKED_DELIVERIES_DEDUP_INDEX: &str = "parked_deliveries_source_kind_identifier_key";

/// Codeberg issue #25 review (docs/architecture.md §56): belt-and-suspenders
/// for `catch_up_cross_context_route`'s own advisory-lock fix above -
/// a `UNIQUE` index on `(source, kind, identifier)` (the tuple that
/// identifies one real failed occurrence, whichever `ParkedDeliveryKind`
/// it is) so `insert_parked_delivery`'s own `ON CONFLICT` can turn any
/// remaining duplicate-insert path, from this bug or a future one, into
/// a harmless upsert instead of a second row.
///
/// A live bounded context that already hit the race this fixes can have
/// real duplicate rows sitting in `parked_deliveries` already - creating
/// the index directly would fail outright against those, turning a
/// silent duplication bug into a hard startup error. So this runs as a
/// real migration, `migrate_idempotency_keys_client_id_scoping`'s own
/// shape: a dedicated transaction, a `pg_advisory_xact_lock` on the
/// schema name (serializing concurrent instances migrating the same
/// bounded context at startup), an idempotency check against
/// `pg_indexes` so an already-migrated schema is a cheap no-op, then the
/// actual work - here, deleting every duplicate but the newest
/// (`(last_failed_at, id)` descending; `id` only to break an exact tie,
/// never itself meaningful) before creating the index. A duplicate row
/// this deletes was never independently actionable - both were always
/// the identical occurrence records under `retryParkedDelivery`'s own
/// terms - so discarding all but one loses no real operator-facing
/// information, unlike `idempotency_keys`' own pre-migration rows.
pub async fn migrate_parked_deliveries_dedup_and_unique_index(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let raw_schema = format!("bc_{bounded_context}");

    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(&raw_schema)
        .execute(&mut *tx)
        .await?;

    let already_migrated: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = $1 AND indexname = $2
        )",
    )
    .bind(&raw_schema)
    .bind(PARKED_DELIVERIES_DEDUP_INDEX)
    .fetch_one(&mut *tx)
    .await?;

    if already_migrated {
        tx.commit().await?;
        return Ok(());
    }

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.parked_deliveries a USING {schema}.parked_deliveries b \
         WHERE a.source = b.source AND a.kind = b.kind AND a.identifier = b.identifier \
         AND (a.last_failed_at, a.id) < (b.last_failed_at, b.id)"
    )))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {PARKED_DELIVERIES_DEDUP_INDEX} \
         ON {schema}.parked_deliveries (source, kind, identifier)"
    )))
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// `id` is generated here (`shared::generate_token_id`, the same opaque-
/// id generator every other synthetic identifier in this codebase
/// already uses - see its own doc comment) rather than left to the
/// caller, since nothing about a parked delivery's own identity needs to
/// be caller-chosen or caller-visible before this call returns it.
/// `ON CONFLICT (source, kind, identifier)` (docs/architecture.md §56)
/// turns a duplicate-occurrence insert into an upsert that refreshes the
/// failure details onto the existing row rather than a second one -
/// `RETURNING` so the id/fields this returns always describe the row
/// that actually exists afterward, real either way (the fresh `id` this
/// generated, or the winning row's own from an earlier insert).
#[allow(clippy::too_many_arguments)]
pub async fn insert_parked_delivery(
    pool: &Pool,
    bounded_context: &str,
    source: &str,
    kind: ParkedDeliveryKind,
    identifier: &str,
    access_token_id: Option<&str>,
    target_bounded_context: Option<&str>,
    target_command_type: Option<&str>,
    request_json: &serde_json::Value,
    error: &str,
    attempt_count: i32,
    first_failed_at: DateTime<Utc>,
    last_failed_at: DateTime<Utc>,
) -> crate::error::Result<ParkedDelivery> {
    let schema = schema_ident(bounded_context);
    let id = crate::shared::generate_token_id();
    let row: ParkedDeliveryRow = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.parked_deliveries \
         (id, source, kind, identifier, access_token_id, target_bounded_context, \
          target_command_type, request_json, error, attempt_count, first_failed_at, \
          last_failed_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
         ON CONFLICT (source, kind, identifier) DO UPDATE SET \
         request_json = EXCLUDED.request_json, \
         error = EXCLUDED.error, \
         attempt_count = EXCLUDED.attempt_count, \
         last_failed_at = EXCLUDED.last_failed_at \
         RETURNING {PARKED_DELIVERY_COLUMNS}"
    )))
    .bind(&id)
    .bind(source)
    .bind(kind.as_str())
    .bind(identifier)
    .bind(access_token_id)
    .bind(target_bounded_context)
    .bind(target_command_type)
    .bind(Json(request_json))
    .bind(error)
    .bind(attempt_count)
    .bind(first_failed_at)
    .bind(last_failed_at)
    .fetch_one(pool)
    .await?;
    Ok(row.into())
}

/// `AdminAccess`-gated `parkedDeliveries(boundedContext:)`'s own read -
/// newest failure first, the order an operator triaging a growing list
/// actually wants.
pub async fn list_parked_deliveries(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<ParkedDelivery>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<ParkedDeliveryRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PARKED_DELIVERY_COLUMNS} FROM {schema}.parked_deliveries \
         ORDER BY last_failed_at DESC"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(ParkedDelivery::from).collect())
}

pub async fn get_parked_delivery(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<ParkedDelivery>> {
    let schema = schema_ident(bounded_context);
    let row: Option<ParkedDeliveryRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PARKED_DELIVERY_COLUMNS} FROM {schema}.parked_deliveries WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(ParkedDelivery::from))
}

/// `retryParkedDelivery`'s own failed-again path - the row stays parked,
/// but its own `error`/`attempt_count`/`last_failed_at` reflect this
/// latest attempt rather than only the original one, so an operator
/// looking at the list sees it was actually retried, not just left
/// alone.
pub async fn record_parked_delivery_retry_failure(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
    error: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.parked_deliveries \
         SET error = $1, attempt_count = attempt_count + 1, last_failed_at = $2 \
         WHERE id = $3"
    )))
    .bind(error)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// `discardParkedDelivery`'s own write, and `retryParkedDelivery`'s own
/// success path (a delivery that finally landed is no longer "stuck",
/// so it leaves this table the same way a resolved
/// `discardProjectionRebuild` row does - no separate `resolved`/
/// `discarded` status to track, `delete_projection_rebuild`'s own
/// register). Returns the row as it was immediately before deletion, the
/// same "hand back what's gone" treatment `discard_projection_rebuild`'s
/// own doc comment gives for the identical reason: the caller (a GraphQL
/// mutation) still needs to render it in its response.
pub async fn delete_parked_delivery(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<ParkedDelivery>> {
    let existing = get_parked_delivery(pool, bounded_context, id).await?;
    if existing.is_none() {
        return Ok(None);
    }
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.parked_deliveries WHERE id = $1"
    )))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(existing)
}

/// One catch-up tick for one registered [`crate::plugin::CrossContextRoute`] -
/// called in a loop, on a timer, by the single shared background task
/// `SkiljBuilder::build()` spawns (docs/architecture.md's own write-up
/// of this pass), the same "one shared task, not one per route" register
/// `catch_up_bounded_context`/`catch_up_snapshots` already use.
///
/// Fetches every `Source` occurrence since this route's own cursor
/// (`list_events_cached`, already filtered to `Source`'s own event type -
/// the identical read path `EventReadToken`-based REST consumption
/// already uses), and for each one, in sequence order: asks the
/// type-erased dispatcher to translate the payload
/// (`CrossContextRouteDispatcher::route`), submits `Target`'s own
/// command through [`decide_and_submit_command`] when it produces one,
/// then advances the cursor to that occurrence's own sequence
/// regardless of the outcome - a skip, a rejection, and an accepted
/// submission are all equally "this occurrence is done", only a real
/// `Err` (a transient failure worth retrying next tick) leaves the
/// cursor where it was, stopping this route's own catch-up for this
/// tick without losing anything (the next tick re-fetches from the same
/// cursor).
///
/// **First-ever tick, `CrossContextRoute::START_FROM != Beginning`**:
/// when `get_cross_context_route_cursor` comes back `None` (no row -
/// this route has never ticked before) and the route asks to start
/// anywhere other than `Beginning`, this tick does no dispatching at
/// all. It instead loads `Source`'s own occurrences once (the identical
/// `list_events_cached` call the normal path below makes, from `-1`) -
/// needed to compute `Latest`'s/`AtTime`'s own seed, paid unconditionally
/// even for `AtSequence` (which doesn't need it) for one shared code
/// path rather than a fourth special case - purely to find the seed
/// sequence, writes the cursor row there (or leaves it at `-1`,
/// unchanged, if there's nothing to seed past), and returns - so every
/// occurrence that already existed at registration time is treated as
/// "already seen" without a single `route()` call or `Target` submission
/// for any of them:
/// - `Latest`: the highest sequence among every loaded occurrence.
/// - `AtSequence(n)`: `n` directly - no need to look at `events` at all,
///   the same "opaque, unvalidated value" treatment
///   `EventReadToken.start_at_sequence` gets.
/// - `AtTime(unix_secs)`: the highest sequence among occurrences whose
///   own `metadata.created_at` is at or before that moment - the
///   identical `at_time_position` computation `event_store::consume_events`
///   makes, minus the scope filter (`CrossContextRoute` has no scope
///   concept to filter by).
///
/// This is the mechanism that stops a brand-new route like
/// `UserRegistered -> SendWelcomeEmail` from emailing every user who has
/// ever registered: exactly one full history load, exactly once, ever,
/// for this route - every later tick reads the real cursor row this
/// seeded and only ever sees genuinely new occurrences, the same as a
/// `Beginning` route always has.
///
/// **Codeberg issue #21**: `decide_and_submit_command`'s own `Err` used
/// to propagate straight out of this function via a bare `?`, which
/// stopped this tick immediately without advancing the cursor - correct
/// as far as it went (a crash-safe "the next tick retries the identical
/// occurrence"), but with no cap: a persistently-failing occurrence
/// blocked this route's cursor forever, retried on every single poll
/// tick, unthrottled. Replaced with `retry_policy` (`skilj_retry::
/// RetryPolicy`): each failure grows the backoff before this route's
/// blocked head-of-line occurrence is attempted again (the "not yet
/// time" early return below, backed by `cross_context_route_cursors`'
/// own new `retry_*` columns), and once the policy is exhausted, the
/// occurrence is recorded as a [`ParkedDelivery`] (`source:
/// "cross-context-route:{route.name}"`, `kind: CrossContextRoute`) and
/// the cursor finally advances past it - the route un-blocks, and an
/// operator gets a visible, retryable/discardable record instead of a
/// route silently stuck forever.
///
/// **Codeberg issue #25 review (docs/architecture.md §56)**: this whole
/// tick - reading `retry_attempt_count`, deciding whether the policy is
/// exhausted, and either recording another backoff or parking - is a
/// plain read-then-write with no lock of its own, so two instances
/// polling the same route concurrently could both read the same
/// almost-exhausted retry state, both independently exhaust it, and both
/// call `insert_parked_delivery` for the identical occurrence: two rows
/// for one real failure. Closed by serializing the *entire* tick per
/// `route_name` with a blocking `pg_advisory_xact_lock` - the identical
/// primitive `lock_read_cursor_for_consume` already uses for its own
/// claim race, just held on a dedicated lock-only transaction here
/// rather than the transaction the protected work itself runs in: this
/// function's own work already spans many independent pooled calls
/// (`decide_and_submit_command` alone opens and commits its own nested
/// transaction), so threading one shared transaction through all of it
/// would be a far larger change than this fix calls for. A lock held on
/// its own connection for the tick's whole duration serializes callers
/// just as effectively, since what needs protecting is "only one
/// instance runs this route's tick at a time," not any single row.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(route = %route.name))]
pub async fn catch_up_cross_context_route(
    pool: &Pool,
    route: &crate::plugin::CrossContextRouteInfo,
    route_dispatcher: &dyn crate::plugin::CrossContextRouteDispatcher,
    command_dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    encryption_master_key: Option<&EncryptionMasterKey>,
    retry_policy: &skilj_retry::RetryPolicy,
) -> crate::error::Result<()> {
    // Held for this whole call's duration on a dedicated transaction
    // that carries no application writes of its own - `pg_advisory_xact_lock`
    // releases automatically when it commits (the success path) or rolls
    // back (dropped on an early `?` return), so there's no separate
    // unlock call to forget on any exit path. See this function's own
    // doc comment above for why the lock's connection is deliberately
    // not the same one the tick's own reads/writes use.
    let mut lock_tx = pool.begin().await?;
    let lock_key = format!(
        "cross_context_route:{}:{}",
        route.source_bounded_context, route.name
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(&lock_key)
        .execute(&mut *lock_tx)
        .await?;
    let result = catch_up_cross_context_route_locked(
        pool,
        route,
        route_dispatcher,
        command_dispatcher,
        projection_dispatcher,
        snapshot_dispatcher,
        broadcaster,
        event_cache,
        encryption_master_key,
        retry_policy,
    )
    .await;
    lock_tx.commit().await?;
    result
}

#[allow(clippy::too_many_arguments)]
async fn catch_up_cross_context_route_locked(
    pool: &Pool,
    route: &crate::plugin::CrossContextRouteInfo,
    route_dispatcher: &dyn crate::plugin::CrossContextRouteDispatcher,
    command_dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    encryption_master_key: Option<&EncryptionMasterKey>,
    retry_policy: &skilj_retry::RetryPolicy,
) -> crate::error::Result<()> {
    let existing_cursor =
        get_cross_context_route_cursor(pool, route.source_bounded_context, route.name).await?;
    let cursor = existing_cursor.unwrap_or(-1);
    let events = list_events_cached(
        pool,
        event_cache,
        route.source_bounded_context,
        route.source_event_type,
        cursor,
    )
    .await?;

    if existing_cursor.is_none()
        && route.start_from != crate::plugin::CrossContextRouteStartFrom::Beginning
    {
        // See this function's own doc comment's "First-ever tick" note -
        // `events` above already is the full history from `-1`, loaded
        // for exactly this purpose; nothing in it gets dispatched.
        let seed = match route.start_from {
            crate::plugin::CrossContextRouteStartFrom::Beginning => {
                unreachable!("excluded by this branch's own condition above")
            }
            crate::plugin::CrossContextRouteStartFrom::Latest => {
                events.iter().map(|e| e.sequence).max().unwrap_or(-1)
            }
            crate::plugin::CrossContextRouteStartFrom::AtSequence(n) => n,
            crate::plugin::CrossContextRouteStartFrom::AtTime(unix_secs) => {
                let threshold = DateTime::from_timestamp(unix_secs, 0).unwrap_or(Utc::now());
                events
                    .iter()
                    .filter(|e| e.metadata.created_at <= threshold)
                    .map(|e| e.sequence)
                    .max()
                    .unwrap_or(-1)
            }
        };
        update_cross_context_route_cursor(
            pool,
            route.source_bounded_context,
            route.name,
            seed,
            Utc::now(),
        )
        .await?;
        return Ok(());
    }

    // Codeberg issue #21 - see this function's own doc comment. Read
    // once per tick, ahead of the loop: only the loop's own first
    // occurrence can ever be a retry of a previously-failed one, since
    // the cursor never advances past a blocked occurrence - the same
    // invariant `cross_context_route_cursors` needing only one row's
    // worth of retry state (not one per occurrence) already relies on.
    let (mut retry_attempt, mut retry_first_failed_at, retry_next_attempt_at) =
        get_cross_context_route_retry_state(pool, route.source_bounded_context, route.name).await?;
    if retry_attempt > 0 {
        if let Some(next_attempt_at) = retry_next_attempt_at {
            if Utc::now() < next_attempt_at {
                // Not yet time - this route's own blocked occurrence is
                // still in backoff. Skip this tick entirely rather than
                // re-attempting the identical failing submission on
                // every single poll.
                return Ok(());
            }
        }
    }

    for event in &events {
        match route_dispatcher.route(route.name, &event.payload) {
            None => {
                // Defensive only - `route.name` came from this same
                // dispatcher's own `routes()` list, so this should never
                // actually happen. Still advances the cursor below
                // rather than looping on it forever.
                tracing::warn!(
                    sequence = event.sequence,
                    "cross-context route not found in its own dispatcher - skipping"
                );
            }
            Some(Err(e)) => {
                // The stored payload didn't deserialize into `Source::
                // Payload` - see `BoundedContextEvent::try_from_event`'s
                // own doc comment on why this is reachable. Retrying can
                // never fix a payload the event itself was stored with,
                // so this is logged and skipped, not retried forever.
                tracing::warn!(
                    sequence = event.sequence,
                    error = %e,
                    "cross-context route: source payload did not deserialize - skipping"
                );
            }
            Some(Ok(None)) => {
                // `route()` itself decided this occurrence doesn't
                // apply - a real, expected outcome, not an error.
            }
            Some(Ok(Some(target_payload))) => {
                let Some(target_command_type) = get_command_type(
                    pool,
                    route.target_bounded_context,
                    route.target_command_type,
                )
                .await?
                else {
                    tracing::warn!(
                        sequence = event.sequence,
                        target_bounded_context = route.target_bounded_context,
                        target_command_type = route.target_command_type,
                        "cross-context route's own target CommandType isn't registered - \
                         skipping this occurrence, cursor still advances"
                    );
                    update_cross_context_route_cursor(
                        pool,
                        route.source_bounded_context,
                        route.name,
                        event.sequence,
                        Utc::now(),
                    )
                    .await?;
                    retry_attempt = 0;
                    retry_first_failed_at = None;
                    continue;
                };
                // Prefixed with `RESERVED_IDEMPOTENCY_KEY_PREFIX` - a
                // Historical note (docs/architecture.md §36/§37): this
                // prefix originally existed because `idempotency_keys`
                // had no caller/client_id column at all, so an ordinary
                // Write-level caller could pre-plant `"{route.name}:{sequence}"`
                // via `submitCommand`/`Idempotency-Key` ahead of time and
                // silently swallow this submission as a `Deduplicated`
                // no-op. `idempotency_keys` is `client_id`-scoped now (this
                // call's own `client_id` below is always `"cross-context-route"`,
                // never externally suppliable), which closes that gap
                // structurally - this reservation is kept as a harmless
                // second layer, not the load-bearing defense it was.
                let idempotency_key = format!(
                    "{}{}:{}",
                    crate::event_store::RESERVED_IDEMPOTENCY_KEY_PREFIX,
                    route.name,
                    event.sequence
                );
                // Codeberg issue #18: the routed command finally gets a
                // real answer to "what caused this" - forward-carrying
                // the source event's own correlation_id (always present
                // by this point) and naming the source event itself as
                // the direct cause, via `event_causation_id`'s composed
                // `{bounded_context}:{sequence}` string (`Event` has no
                // synthetic id of its own to use instead - see that
                // helper's own doc comment).
                let outcome = decide_and_submit_command(
                    pool,
                    command_dispatcher,
                    projection_dispatcher,
                    snapshot_dispatcher,
                    broadcaster,
                    event_cache,
                    &target_command_type,
                    &target_payload,
                    "cross-context-route",
                    event.metadata.correlation_id.as_deref(),
                    Some(&crate::event_store::event_causation_id(event)),
                    encryption_master_key,
                    Utc::now(),
                    Some(&idempotency_key),
                )
                .await;
                // Codeberg issue #21 - see this function's own doc
                // comment. `Err` no longer propagates straight out via
                // `?`; it's retried with backoff, up to `retry_policy`,
                // before this occurrence is parked.
                let outcome = match outcome {
                    Ok(outcome) => outcome,
                    Err(e) => {
                        retry_attempt += 1;
                        // Every path out of this `Err` arm either
                        // `continue`s (after resetting `retry_attempt`/
                        // `retry_first_failed_at` to their defaults - the
                        // occurrence is done, parked) or `return`s (the
                        // function exits before the outer
                        // `retry_first_failed_at` would ever be read
                        // again) - so unlike `retry_attempt`, there's no
                        // corresponding outer reassignment needed for
                        // `first_failed_at` here, only this local,
                        // already-resolved value.
                        let first_failed_at = retry_first_failed_at.unwrap_or_else(Utc::now);
                        let now = Utc::now();
                        let elapsed = (now - first_failed_at).to_std().unwrap_or_default();
                        if retry_policy.is_exhausted(retry_attempt as u32, elapsed) {
                            tracing::error!(
                                sequence = event.sequence,
                                error = %e,
                                attempt = retry_attempt,
                                "cross-context route: target command submission failed \
                                 repeatedly - parking and skipping this occurrence"
                            );
                            let request_json = serde_json::from_str(&target_payload)
                                .unwrap_or(serde_json::Value::String(target_payload));
                            insert_parked_delivery(
                                pool,
                                route.target_bounded_context,
                                &format!("cross-context-route:{}", route.name),
                                ParkedDeliveryKind::CrossContextRoute,
                                &event.sequence.to_string(),
                                None,
                                Some(route.target_bounded_context),
                                Some(route.target_command_type),
                                &request_json,
                                &e.to_string(),
                                retry_attempt,
                                first_failed_at,
                                now,
                            )
                            .await?;
                            update_cross_context_route_cursor(
                                pool,
                                route.source_bounded_context,
                                route.name,
                                event.sequence,
                                now,
                            )
                            .await?;
                            retry_attempt = 0;
                            retry_first_failed_at = None;
                            continue;
                        }
                        let next_attempt_at = now
                            + chrono::Duration::from_std(
                                retry_policy.next_backoff(retry_attempt as u32),
                            )
                            .unwrap_or(chrono::Duration::zero());
                        tracing::warn!(
                            sequence = event.sequence,
                            error = %e,
                            attempt = retry_attempt,
                            next_attempt_at = %next_attempt_at,
                            "cross-context route: target command submission failed - will \
                             retry with backoff; route blocked until then"
                        );
                        record_cross_context_route_retry_failure(
                            pool,
                            route.source_bounded_context,
                            route.name,
                            cursor,
                            retry_attempt,
                            first_failed_at,
                            next_attempt_at,
                            now,
                        )
                        .await?;
                        return Ok(());
                    }
                };
                // Defence in depth, not solely a bypass signal: with the
                // prefix reserved, nothing *else* can write into this
                // namespace, but this route's own past attempt at this
                // exact occurrence can - a process crash/restart between
                // this submission committing and the cursor advancing
                // below leaves the cursor pointing at this same source
                // event, so the next tick re-derives the identical key
                // and correctly lands here. That's ordinary, benign
                // crash recovery, not an anomaly - warned rather than
                // errored either way, since the cursor still correctly
                // advances past this occurrence.
                if matches!(outcome, SubmitCommandOutcome::Deduplicated { .. }) {
                    tracing::warn!(
                        sequence = event.sequence,
                        "cross-context route's own idempotency key was already present - \
                         expected after a crash/restart between a prior submission and its \
                         cursor advance; unexpected otherwise, since the key space is reserved"
                    );
                }
            }
        }
        update_cross_context_route_cursor(
            pool,
            route.source_bounded_context,
            route.name,
            event.sequence,
            Utc::now(),
        )
        .await?;
        retry_attempt = 0;
        retry_first_failed_at = None;
    }
    Ok(())
}

// --- Codeberg issue #20: native one-shot, per-entity deadlines ---
//
// `catch_up_schedule_deadline`/`catch_up_cancel_deadline` are
// `catch_up_cross_context_route`'s own shape, applied to
// `ScheduleDeadline`/`CancelDeadline` instead of `CrossContextRoute` -
// walk one registered reactor's own `Source` event stream after its
// cursor, react to every occurrence, advance the cursor past it either
// way. `fire_due_deadlines` has no `CrossContextRoute` analogue: it
// isn't tied to any one registered type, it scans the `deadlines` table
// itself by `fire_at`, the same register `scheduler_tick_for_bounded_context`'s
// own due-occurrence scan already is.

/// `deadline_cursors`' own read - `get_cross_context_route_cursor`'s
/// analogue, `cursor_owner` a `ScheduleDeadline::NAME`/`CancelDeadline::NAME`.
async fn get_deadline_cursor(
    pool: &Pool,
    bounded_context: &str,
    cursor_owner: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT last_dispatched_sequence FROM {schema}.deadline_cursors WHERE cursor_owner = $1"
    )))
    .bind(cursor_owner)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(seq,)| seq))
}

async fn update_deadline_cursor(
    pool: &Pool,
    bounded_context: &str,
    cursor_owner: &str,
    sequence: i64,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.deadline_cursors \
         (cursor_owner, last_dispatched_sequence, updated_at) VALUES ($1, $2, $3) \
         ON CONFLICT (cursor_owner) DO UPDATE SET \
         last_dispatched_sequence = EXCLUDED.last_dispatched_sequence, \
         updated_at = EXCLUDED.updated_at"
    )))
    .bind(cursor_owner)
    .bind(sequence)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// One registered `ScheduleDeadline`'s own catch-up tick - see this
/// section's own header comment, and `catch_up_cross_context_route`'s
/// doc comment for the full "first-ever tick seeding" reasoning
/// `start_from != Beginning` shares with it verbatim.
pub async fn catch_up_schedule_deadline(
    pool: &Pool,
    schedule: &crate::plugin::ScheduleDeadlineInfo,
    dispatcher: &dyn crate::plugin::ScheduleDeadlineDispatcher,
    event_cache: &crate::event_cache::EventCache,
) -> crate::error::Result<()> {
    let existing_cursor =
        get_deadline_cursor(pool, schedule.source_bounded_context, schedule.name).await?;
    let cursor = existing_cursor.unwrap_or(-1);
    let events = list_events_cached(
        pool,
        event_cache,
        schedule.source_bounded_context,
        schedule.source_event_type,
        cursor,
    )
    .await?;

    if existing_cursor.is_none()
        && schedule.start_from != crate::plugin::DeadlinePollStartFrom::Beginning
    {
        let seed = match schedule.start_from {
            crate::plugin::DeadlinePollStartFrom::Beginning => {
                unreachable!("excluded by this branch's own condition above")
            }
            crate::plugin::DeadlinePollStartFrom::Latest => {
                events.iter().map(|e| e.sequence).max().unwrap_or(-1)
            }
            crate::plugin::DeadlinePollStartFrom::AtSequence(n) => n,
            crate::plugin::DeadlinePollStartFrom::AtTime(unix_secs) => {
                let threshold = DateTime::from_timestamp(unix_secs, 0).unwrap_or(Utc::now());
                events
                    .iter()
                    .filter(|e| e.metadata.created_at <= threshold)
                    .map(|e| e.sequence)
                    .max()
                    .unwrap_or(-1)
            }
        };
        update_deadline_cursor(
            pool,
            schedule.source_bounded_context,
            schedule.name,
            seed,
            Utc::now(),
        )
        .await?;
        return Ok(());
    }

    let schema = schema_ident(schedule.source_bounded_context);
    for event in &events {
        match dispatcher.schedule(schedule.name, &event.payload) {
            None => {
                // Defensive only - `schedule.name` came from this same
                // dispatcher's own `schedules()` list.
                tracing::warn!(
                    sequence = event.sequence,
                    "schedule deadline not found in its own dispatcher - skipping"
                );
            }
            Some(Err(e)) => {
                tracing::warn!(
                    sequence = event.sequence,
                    error = %e,
                    "schedule deadline: source payload did not deserialize - skipping"
                );
            }
            Some(Ok(None)) => {
                // `schedule()` itself decided this occurrence doesn't
                // apply - a real, expected outcome, not an error.
            }
            Some(Ok(Some(spec))) => {
                // Deterministic id - see `ensure_deadlines_table`'s own
                // doc comment for why `ON CONFLICT (id) DO NOTHING` makes
                // a redelivered tick a safe no-op.
                let id = format!("{}:{}", schedule.name, event.sequence);
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {schema}.deadlines \
                     (id, schedule_name, fire_at, tags, correlation_id, target_bounded_context, \
                      target_command_type, payload, status, created_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9) \
                     ON CONFLICT (id) DO NOTHING"
                )))
                .bind(&id)
                .bind(schedule.name)
                .bind(spec.fire_at)
                .bind(Json(&spec.tags))
                .bind(event.metadata.correlation_id.as_deref())
                .bind(schedule.target_bounded_context)
                .bind(schedule.target_command_type)
                .bind(&spec.payload_json)
                .bind(Utc::now())
                .execute(pool)
                .await?;
            }
        }
        update_deadline_cursor(
            pool,
            schedule.source_bounded_context,
            schedule.name,
            event.sequence,
            Utc::now(),
        )
        .await?;
    }
    Ok(())
}

/// One registered `CancelDeadline`'s own catch-up tick - same shape as
/// `catch_up_schedule_deadline` just above, reacting by cancelling
/// pending rows instead of inserting one. Writes into
/// `cancel.deadline_schedule_bounded_context`'s own `deadlines` table,
/// which can differ from `cancel.source_bounded_context` - see
/// `CancelDeadlineInfo::deadline_schedule_bounded_context`'s own doc
/// comment.
pub async fn catch_up_cancel_deadline(
    pool: &Pool,
    cancel: &crate::plugin::CancelDeadlineInfo,
    dispatcher: &dyn crate::plugin::CancelDeadlineDispatcher,
    event_cache: &crate::event_cache::EventCache,
) -> crate::error::Result<()> {
    let existing_cursor =
        get_deadline_cursor(pool, cancel.source_bounded_context, cancel.name).await?;
    let cursor = existing_cursor.unwrap_or(-1);
    let events = list_events_cached(
        pool,
        event_cache,
        cancel.source_bounded_context,
        cancel.source_event_type,
        cursor,
    )
    .await?;

    if existing_cursor.is_none()
        && cancel.start_from != crate::plugin::DeadlinePollStartFrom::Beginning
    {
        let seed = match cancel.start_from {
            crate::plugin::DeadlinePollStartFrom::Beginning => {
                unreachable!("excluded by this branch's own condition above")
            }
            crate::plugin::DeadlinePollStartFrom::Latest => {
                events.iter().map(|e| e.sequence).max().unwrap_or(-1)
            }
            crate::plugin::DeadlinePollStartFrom::AtSequence(n) => n,
            crate::plugin::DeadlinePollStartFrom::AtTime(unix_secs) => {
                let threshold = DateTime::from_timestamp(unix_secs, 0).unwrap_or(Utc::now());
                events
                    .iter()
                    .filter(|e| e.metadata.created_at <= threshold)
                    .map(|e| e.sequence)
                    .max()
                    .unwrap_or(-1)
            }
        };
        update_deadline_cursor(
            pool,
            cancel.source_bounded_context,
            cancel.name,
            seed,
            Utc::now(),
        )
        .await?;
        return Ok(());
    }

    let target_schema = schema_ident(cancel.deadline_schedule_bounded_context);
    for event in &events {
        match dispatcher.cancel_tags(cancel.name, &event.payload) {
            None => {
                tracing::warn!(
                    sequence = event.sequence,
                    "cancel deadline not found in its own dispatcher - skipping"
                );
            }
            Some(Err(e)) => {
                tracing::warn!(
                    sequence = event.sequence,
                    error = %e,
                    "cancel deadline: source payload did not deserialize - skipping"
                );
            }
            Some(Ok(None)) => {
                // `cancel_tags()` itself decided this occurrence doesn't
                // apply.
            }
            Some(Ok(Some(tags))) if tags.is_empty() => {
                // An empty tag list matches nothing - the identical
                // treatment `list_events_for_bounded_context_matching_tags`'s
                // own early return already gives, applied here rather
                // than running a `WHERE` clause with no tag condition at
                // all (which would match every still-pending row this
                // schedule owns, not none).
            }
            Some(Ok(Some(tags))) => {
                // Tag-containment, one `tags @> $n::jsonb` clause per
                // wanted tag, ORed together - the identical shape
                // `list_events_for_bounded_context_matching_tags` already
                // uses against the `events` table's own `tags` column
                // (docs/architecture.md §19 Problem 1), applied here to
                // `deadlines.tags` instead. Cancels every still-`pending`
                // row this schedule owns that shares at least one of
                // these tags - zero, one, or several rows, all a
                // legitimate outcome (see `CancelDeadline::cancel_tags`'s
                // own doc comment).
                let tag_literals: Vec<String> = tags
                    .iter()
                    .map(|t| {
                        serde_json::to_string(std::slice::from_ref(t))
                            .expect("Tag serialisation is infallible")
                    })
                    .collect();
                let tag_clause = (0..tag_literals.len())
                    .map(|i| format!("tags @> ${}::jsonb", i + 3))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let mut query = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {target_schema}.deadlines SET status = 'cancelled', resolved_at = $1 \
                     WHERE schedule_name = $2 AND status = 'pending' AND ({tag_clause})"
                )))
                .bind(Utc::now())
                .bind(cancel.deadline_schedule_name);
                for literal in tag_literals {
                    query = query.bind(literal);
                }
                query.execute(pool).await?;
            }
        }
        update_deadline_cursor(
            pool,
            cancel.source_bounded_context,
            cancel.name,
            event.sequence,
            Utc::now(),
        )
        .await?;
    }
    Ok(())
}

/// `fire_due_deadlines`'s own per-tick cap - the identical "recent
/// window, not a hard limit on correctness" register
/// `scheduler_tick`'s own `MAX_OCCURRENCES_PER_TICK` already is: a
/// backlog bigger than this just takes more ticks to drain, each one
/// picking up wherever the last left off (`fire_at` order, never
/// re-offering an already-`fired`/`cancelled` row).
const MAX_DUE_DEADLINES_PER_TICK: i64 = 1000;

/// How long a claimed (`'firing'`) row is left alone before a future
/// tick is allowed to reclaim it - long enough that a live instance's
/// own `decide_and_submit_command` call (a handful of DB round trips)
/// never comes close, short enough that a genuinely crashed claim (the
/// only way a row is still `'firing'` this long after `firing_at`) gets
/// retried on a human-visible timescale rather than parked forever. The
/// retry itself is safe even if the original attempt actually did
/// commit its command before crashing: `RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX`
/// makes the resubmission a harmless `Deduplicated`, the same
/// defense-in-depth `fire_due_deadlines`'s own doc comment already
/// describes for the fire-vs-fire case this claim mechanism otherwise
/// closes off entirely.
fn deadline_firing_claim_stale_after() -> chrono::Duration {
    chrono::Duration::minutes(5)
}

#[derive(sqlx::FromRow)]
struct DueDeadlineRow {
    id: String,
    correlation_id: Option<String>,
    target_bounded_context: String,
    target_command_type: String,
    payload: String,
}

/// One bounded context's own share of the firing scan - **not** tied to
/// any one registered `ScheduleDeadline`, unlike the two catch-up
/// functions above: it scans `{schema}.deadlines` itself, so a row fires
/// regardless of whether the `ScheduleDeadline` that created it is still
/// registered in this process (the same "every instance does the same
/// redundant, idempotent work" register [docs/architecture.md §22](../../../docs/architecture.md#background-polling-and-startup-scaling)
/// already established.
///
/// No `FOR UPDATE SKIP LOCKED` on the initial `SELECT` - two instances
/// racing to fire the same row both submit under the identical
/// idempotency key, so the second is a harmless `Deduplicated`. But
/// unlike that fire-vs-fire race, a fire-vs-*cancel* race is not
/// harmless: `catch_up_cancel_deadline` can flip this same row to
/// `'cancelled'` concurrently, and if that landed between this
/// function's own `SELECT` and its call to `decide_and_submit_command`,
/// the row would end up recorded `'cancelled'` while the target command
/// had already been submitted - see [docs/architecture.md §55](../../../docs/architecture.md#55-closing-the-canceldeadline-fire-vs-cancel-race).
/// Each row is therefore atomically claimed (`'pending'` -> `'firing'`)
/// immediately before submitting, via the same `UPDATE ... WHERE
/// status = 'pending' RETURNING` shape `mark_deadline_resolved` already
/// uses to make its own write idempotent. `catch_up_cancel_deadline`'s
/// own `UPDATE ... WHERE status = 'pending'` then naturally loses the
/// race once a row is `'firing'` - no change needed there, only a
/// dedicated test proving it (`skilj/tests/deadlines.rs`).
///
/// A due row whose own `target_command_type` isn't registered at all is
/// marked `fired` without ever calling `decide_and_submit_command` -
/// logged as a warning, not retried forever, the identical stance
/// `catch_up_cross_context_route` already takes for its own "target
/// `CommandType` isn't registered" case. No claim needed on that path:
/// no side effect has happened yet, so losing a race with a concurrent
/// cancel there is just ordinary "cancelled before it fired" - the
/// correct outcome, not a bug - so `mark_deadline_resolved` is called
/// directly against the still-`'pending'` row. A `Target` command that
/// *is* submitted but gets rejected by its own `decide()` is marked
/// `fired` too - a legitimate business outcome (`ScheduleDeadline`'s own
/// doc comment), not a reason to retry.
#[allow(clippy::too_many_arguments)]
pub async fn fire_due_deadlines(
    pool: &Pool,
    command_dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    now: DateTime<Utc>,
    encryption_master_key: Option<&EncryptionMasterKey>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let stale_cutoff = now - deadline_firing_claim_stale_after();
    let rows: Vec<DueDeadlineRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, correlation_id, target_bounded_context, target_command_type, payload \
         FROM {schema}.deadlines \
         WHERE (status = 'pending' OR (status = 'firing' AND firing_at <= $1)) AND fire_at <= $2 \
         ORDER BY fire_at LIMIT {MAX_DUE_DEADLINES_PER_TICK}"
    )))
    .bind(stale_cutoff)
    .bind(now)
    .fetch_all(pool)
    .await?;

    for row in rows {
        let Some(target_command_type) =
            get_command_type(pool, &row.target_bounded_context, &row.target_command_type).await?
        else {
            tracing::warn!(
                deadline_id = %row.id,
                target_bounded_context = %row.target_bounded_context,
                target_command_type = %row.target_command_type,
                "deadline's own target CommandType isn't registered - marking fired without submitting"
            );
            mark_deadline_resolved(pool, &schema, &row.id, "fired", now).await?;
            continue;
        };
        // Claims a `'pending'` row outright, or reclaims a `'firing'` one
        // whose own `firing_at` is already past `stale_cutoff` - the
        // identical condition the `SELECT` above already filtered on,
        // re-checked here under the `UPDATE`'s own row lock so a second
        // instance racing to reclaim the same stale row always loses
        // (Postgres re-evaluates `WHERE` against the just-committed row
        // once the first `UPDATE` releases it, and by then `firing_at`
        // is `now`, no longer `<= stale_cutoff`).
        let claimed: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.deadlines SET status = 'firing', firing_at = $1 \
             WHERE id = $2 AND (status = 'pending' OR (status = 'firing' AND firing_at <= $3)) \
             RETURNING id"
        )))
        .bind(now)
        .bind(&row.id)
        .bind(stale_cutoff)
        .fetch_optional(pool)
        .await?;
        if claimed.is_none() {
            // Lost the claim to a concurrent cancel, or to a concurrent
            // instance's own fire/reclaim attempt on the same row -
            // either way, correct to skip: this instance must not submit
            // the target command.
            continue;
        }
        // Prefixed with `RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX` - see
        // that constant's own doc comment for why this is
        // defense-in-depth, not load-bearing, now that `idempotency_keys`
        // is `client_id`-scoped and this call's own `client_id` below
        // ("deadline") is never externally suppliable.
        let idempotency_key = format!(
            "{}{}",
            crate::event_store::RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX,
            row.id
        );
        // Codeberg issue #18: carries the scheduling event's own
        // correlation_id forward (stored on the row at schedule time) -
        // no `causation_id` of its own, since there's no `Event` this
        // firing directly descends from the way a `CrossContextRoute`'s
        // own submission descends from the `Source` event that triggered
        // it (`event_causation_id`); a fired deadline's real cause is a
        // clock, not a prior commit.
        let outcome = decide_and_submit_command(
            pool,
            command_dispatcher,
            projection_dispatcher,
            snapshot_dispatcher,
            broadcaster,
            event_cache,
            &target_command_type,
            &row.payload,
            "deadline",
            row.correlation_id.as_deref(),
            None,
            encryption_master_key,
            now,
            Some(&idempotency_key),
        )
        .await?;
        if matches!(outcome, SubmitCommandOutcome::Deduplicated { .. }) {
            tracing::warn!(
                deadline_id = %row.id,
                "deadline's own idempotency key was already present - expected after a \
                 crash/restart between a prior fire attempt and marking it resolved; \
                 unexpected otherwise, since the key space is reserved"
            );
        }
        mark_deadline_resolved(pool, &schema, &row.id, "fired", now).await?;
    }
    Ok(())
}

/// `WHERE status IN ('pending', 'firing')` - called both directly
/// against a still-`'pending'` row (the "target `CommandType` isn't
/// registered" path, no claim taken) and against a row this same tick
/// already claimed into `'firing'` (the normal fire path) - either way
/// idempotent: a status that's already terminal (`'fired'`/`'cancelled'`)
/// never matches, so a redelivered/duplicate call is a safe no-op.
async fn mark_deadline_resolved(
    pool: &Pool,
    schema: &str,
    id: &str,
    status: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.deadlines SET status = $1, resolved_at = $2 \
         WHERE id = $3 AND status IN ('pending', 'firing')"
    )))
    .bind(status)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// A hit returns the stored `triggered_event_sequences` from a prior
/// `Accepted` outcome for this exact `(command_type_name, client_id,
/// idempotency_key)` triple - `submit_command`'s own short-circuit. Must
/// only be called after the bounded context's own `sequence` row lock
/// is already held (see `submit_command`'s own doc comment) - that lock
/// is what makes this plain, unlocked `SELECT` race-free, the same way
/// it already makes the DCB-conflict recheck a few lines below it
/// race-free.
///
/// A real `client_id`-only match, never a `client_id = ''` legacy row -
/// see `migrate_idempotency_keys_client_id_scoping`'s own doc comment
/// for why a pre-migration row is deliberately left permanently
/// unmatchable rather than kept as a fallback: the user's own explicit
/// call, choosing to fully close the cross-tenant collision this whole
/// fix exists for over preserving those specific rows' dedup power.
async fn lookup_idempotency_key<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    schema: &str,
    command_type_name: &str,
    client_id: &str,
    idempotency_key: &str,
) -> crate::error::Result<Option<Vec<i64>>> {
    let row: Option<(Vec<i64>,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT triggered_event_sequences FROM {schema}.idempotency_keys \
         WHERE command_type_name = $1 AND client_id = $2 AND idempotency_key = $3"
    )))
    .bind(command_type_name)
    .bind(client_id)
    .bind(idempotency_key)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|(sequences,)| sequences))
}

/// Records a real `Accepted` outcome against its idempotency key, inside
/// the same transaction as the `Command`/`Event` rows it describes - a
/// later duplicate submission bearing this key from this same
/// `client_id` short-circuits to `triggered_event_sequences` via
/// `lookup_idempotency_key` instead of being re-decided. No `ON
/// CONFLICT` - the sequence row lock already rules out a concurrent
/// duplicate reaching here (`lookup_idempotency_key` would already have
/// caught it); a real conflict here would mean a bug in that check,
/// worth surfacing as a hard error rather than silently swallowing.
async fn insert_idempotency_key<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    schema: &str,
    command_type_name: &str,
    client_id: &str,
    idempotency_key: &str,
    triggered_event_sequences: &[i64],
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.idempotency_keys \
         (command_type_name, client_id, idempotency_key, triggered_event_sequences, created_at) \
         VALUES ($1, $2, $3, $4, $5)"
    )))
    .bind(command_type_name)
    .bind(client_id)
    .bind(idempotency_key)
    .bind(triggered_event_sequences)
    .bind(now)
    .execute(executor)
    .await?;
    Ok(())
}

/// Codeberg issue #15: the batched replacement for looping every
/// registered projection through `get_projection_rebuild` - that path
/// cost up to `~2P` queries per tick for `P` projections even when
/// nothing was building (`get_projection_rebuild` alone is 3 queries:
/// `get_projection`, the rebuild row, `rebuild_consumed_event_types` -
/// and that last one has its own inner N+1, one `get_event_type` call
/// per consumed event type name). This does the same job in a small
/// constant number of queries regardless of `P` or how many rebuilds
/// are actually building: one query for every `building` row in the
/// bc's `projection_rebuilds` table, one for every row in
/// `projection_rebuild_consumed_event_types` (grouped by
/// `projection_name` in memory), one for every `EventType` in the bc
/// (`list_event_types_for_bounded_context`, already a single query),
/// and zero further queries for the live `Projection` each rebuild
/// belongs to - `all_projections` is data the caller
/// (`catch_up_bounded_context`) already fetched, handed in here instead
/// of re-fetched via `get_project` per row the way `get_projection_rebuild`
/// does for its own single-row callers (`RebuildProjection`/
/// `DiscardProjectionRebuild`'s resolvers - untouched, a single lookup
/// is the right shape there).
async fn list_building_projection_rebuilds_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
    all_projections: &[Projection],
) -> crate::error::Result<Vec<ProjectionRebuild>> {
    let schema = schema_ident(bounded_context);

    let rebuild_rows: Vec<ProjectionRebuildRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_REBUILD_COLUMNS} FROM {schema}.projection_rebuilds \
         WHERE status = 'building'"
    )))
    .fetch_all(pool)
    .await?;
    if rebuild_rows.is_empty() {
        return Ok(Vec::new());
    }

    let consumed_rows: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT projection_name, event_type_name \
         FROM {schema}.projection_rebuild_consumed_event_types WHERE status = 'building'"
    )))
    .fetch_all(pool)
    .await?;
    let mut consumed_by_projection: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for (projection_name, event_type_name) in consumed_rows {
        consumed_by_projection
            .entry(projection_name)
            .or_default()
            .push(event_type_name);
    }

    let all_event_types = list_event_types_for_bounded_context(pool, bounded_context).await?;
    let event_types_by_name: std::collections::HashMap<&str, &EventType> = all_event_types
        .iter()
        .map(|et| (et.name.as_str(), et))
        .collect();

    let projections_by_name: std::collections::HashMap<&str, &Projection> = all_projections
        .iter()
        .map(|p| (p.name.as_str(), p))
        .collect();

    let mut rebuilds = Vec::with_capacity(rebuild_rows.len());
    for row in rebuild_rows {
        let Some(projection) = projections_by_name.get(row.projection_name.as_str()) else {
            continue;
        };
        let consumed_event_types = consumed_by_projection
            .remove(&row.projection_name)
            .unwrap_or_default()
            .into_iter()
            .map(|name| {
                event_types_by_name
                    .get(name.as_str())
                    .copied()
                    .cloned()
                    .expect(
                        "projection_rebuild_consumed_event_types join row references an \
                         event_types row that no longer exists",
                    )
            })
            .collect();
        rebuilds.push(ProjectionRebuild {
            projection: (*projection).clone(),
            schema: row.schema,
            schema_version: row.schema_version,
            consumed_event_types,
            sync: row.sync,
            caught_up_to: row.caught_up_to,
            status: projection_rebuild_status_from_str(&row.status),
        });
    }
    Ok(rebuilds)
}

/// The background half of [§8](../../../docs/architecture.md#open-for-a-future-pass) item 6: one poll tick, for one bounded
/// context - folds every committed event not yet reflected in that
/// context's `sync = false` `Projection`s, and separately walks any
/// `building` `ProjectionRebuild`s toward completion, promoting each one
/// automatically once it catches up to the same high-water mark. Called
/// in a loop, on a timer, by the single shared background task
/// `SkiljBuilder::build()` spawns - see that function's own doc comment
/// for why one shared task rather than one per bounded context.
///
/// Uses the `-1` "nothing yet" sentinel `next_sequence`'s own `sequence`
/// row is seeded with throughout, rather than threading `Option<i64>`
/// through every comparison - `caught_up_to.unwrap_or(-1)` and
/// `latest_sequence(...).unwrap_or(-1)` compare directly, and a
/// building rebuild in a bounded context with no events at all (`latest
/// = -1`) is then trivially already caught up (`caught_up_to.unwrap_or(-1)
/// == -1`) and promotes immediately, with no separate "nothing committed
/// yet" special case needed.
///
/// A rebuild whose `caught_up_to` is `None` - freshly staged, or just
/// restaged while `building` (`upsert_projection_rebuild`'s own doc
/// comment) - has its `projection_rebuild_state` row deleted up front,
/// before folding anything: `ProjectionDispatcher::default_state`
/// resolved fresh, right before the first event actually gets folded, is
/// the only correct starting point once that reset has happened; a row
/// left over from a build attempt this same reset just invalidated would
/// otherwise be silently reused as if it were still current. A rebuild
/// this dispatcher can't resolve at all (no compiled type registered in
/// this process for that name) still advances - `state` frozen at `"{}"`,
/// a neutral placeholder never actually deserialised by anything, since
/// `dispatcher.project()` returns `None` for the same lookup key
/// `default_state()` did - the same "position always advances, state
/// only changes when the dispatcher can" treatment sync projections
/// already get from `insert_event_and_update_sync_projections` above.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn catch_up_bounded_context(
    pool: &Pool,
    bounded_context: &str,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let latest = latest_sequence(pool, bounded_context).await?.unwrap_or(-1);

    let all_projections = list_projections_for_bounded_context(pool, bounded_context).await?;
    // Codeberg issue #25 (docs/architecture.md §51) - every async
    // projection is split here by its own `Projection::PARTITION_COUNT`
    // (via the dispatcher, since that's a Rust-only config the domain
    // `Projection` struct itself never carries). `unpartitioned_async_projections`
    // (`<= 1`, the default, and every projection that predates this
    // feature) feeds the exact same loop this function has always had,
    // completely unchanged below - order is kept simply by never
    // entering the new code path. `.max(1)` guards a misconfigured
    // `PARTITION_COUNT = 0` against a later divide-by-zero.
    let unpartitioned_async_projections: Vec<_> = all_projections
        .iter()
        .filter(|p| {
            !p.sync
                && dispatcher
                    .partition_count(bounded_context, &p.name)
                    .unwrap_or(1)
                    .max(1)
                    <= 1
        })
        .collect();
    let partitioned_async_projections: Vec<(&Projection, u32)> = all_projections
        .iter()
        .filter(|p| !p.sync)
        .filter_map(|p| {
            let partition_count = dispatcher
                .partition_count(bounded_context, &p.name)
                .unwrap_or(1)
                .max(1);
            (partition_count > 1).then_some((p, partition_count))
        })
        .collect();
    let building_rebuilds = list_building_projection_rebuilds_for_bounded_context(
        pool,
        bounded_context,
        &all_projections,
    )
    .await?;

    if unpartitioned_async_projections.is_empty()
        && partitioned_async_projections.is_empty()
        && building_rebuilds.is_empty()
    {
        return Ok(());
    }

    for rebuild in &building_rebuilds {
        if rebuild.caught_up_to.is_none() {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {schema}.projection_rebuild_state \
                 WHERE projection_name = $1 AND status = 'building'"
            )))
            .bind(&rebuild.projection.name)
            .execute(pool)
            .await?;
        }
    }

    let min_caught_up = unpartitioned_async_projections
        .iter()
        .map(|p| p.caught_up_to.unwrap_or(-1))
        .chain(
            building_rebuilds
                .iter()
                .map(|r| r.caught_up_to.unwrap_or(-1)),
        )
        .min()
        .unwrap_or(latest);

    let events = if min_caught_up >= latest {
        Vec::new()
    } else {
        list_events_for_bounded_context_from(pool, bounded_context, min_caught_up).await?
    };

    for event in &events {
        let mut tx = pool.begin().await?;

        for projection in &unpartitioned_async_projections {
            if projection.caught_up_to.unwrap_or(-1) >= event.sequence {
                continue;
            }

            // `Some(vec![])`/`None` both mean zero instances touched -
            // see `insert_event_and_update_sync_projections`'s own
            // identical comment.
            let keys = dispatcher
                .keys(bounded_context, &projection.name, event)
                .unwrap_or_default();
            let default_state_json = dispatcher
                .default_state(bounded_context, &projection.name)
                .unwrap_or_default();
            let owner_tag_key = dispatcher
                .owner_tag_key(bounded_context, &projection.name)
                .flatten();

            for key in &keys {
                // `as_of_sequence` guard (Codeberg issue #25's
                // investigation finding) - this row's own real, current
                // position, re-read fresh under this row's lock rather
                // than trusted from `async_projections`' own function-
                // entry snapshot above (which two concurrent instances'
                // calls would each load independently, stale relative to
                // each other). Proven necessary by a real concurrent
                // test, not just reasoned about: without this check, two
                // instances racing this same row both fold the same
                // event, the second reading the first's already-updated
                // `state` back as `current_state` and folding again on
                // top of it.
                let (as_of_sequence, current_state) = get_or_create_projection_state_for_update(
                    &mut *tx,
                    &schema,
                    &projection.name,
                    key,
                    &default_state_json,
                )
                .await?;
                if as_of_sequence >= event.sequence {
                    continue;
                }

                let new_state = match dispatcher.project(
                    bounded_context,
                    &projection.name,
                    &current_state,
                    event,
                    key,
                ) {
                    Some(result) => result?,
                    None => current_state,
                };

                apply_projection_fold_update(
                    &mut *tx,
                    &schema,
                    "projection_state",
                    "",
                    &projection.name,
                    key,
                    &new_state,
                    owner_tag_key,
                    event,
                )
                .await?;
            }

            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.projections SET caught_up_to = $1 WHERE name = $2"
            )))
            .bind(event.sequence)
            .bind(&projection.name)
            .execute(&mut *tx)
            .await?;
        }

        for rebuild in &building_rebuilds {
            if rebuild.caught_up_to.unwrap_or(-1) >= event.sequence {
                continue;
            }

            let keys = dispatcher
                .keys(bounded_context, &rebuild.projection.name, event)
                .unwrap_or_default();
            let default_state_json = dispatcher
                .default_state(bounded_context, &rebuild.projection.name)
                .unwrap_or_default();
            let owner_tag_key = dispatcher
                .owner_tag_key(bounded_context, &rebuild.projection.name)
                .flatten();

            for key in &keys {
                // `as_of_sequence` guard - see the identical comment on
                // the live-projection loop above; the same cross-instance
                // race applies here for a `ProjectionRebuild`'s own state.
                let (as_of_sequence, current_state) =
                    get_or_create_projection_rebuild_state_for_update(
                        &mut *tx,
                        &schema,
                        &rebuild.projection.name,
                        key,
                        &default_state_json,
                    )
                    .await?;
                if as_of_sequence >= event.sequence {
                    continue;
                }

                let new_state = match dispatcher.project(
                    bounded_context,
                    &rebuild.projection.name,
                    &current_state,
                    event,
                    key,
                ) {
                    Some(result) => result?,
                    None => current_state,
                };

                apply_projection_fold_update(
                    &mut *tx,
                    &schema,
                    "projection_rebuild_state",
                    " AND status = 'building'",
                    &rebuild.projection.name,
                    key,
                    &new_state,
                    owner_tag_key,
                    event,
                )
                .await?;
            }

            // `AND status = 'building'` - not just `projection_name` -
            // matters for real now that a coexisting pending row can
            // share that same `projection_name`: without it, this would
            // also stamp the pending row's own `caught_up_to`, which
            // means nothing for a row that is never folded and must stay
            // `None` until it is promoted or discarded.
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.projection_rebuilds SET caught_up_to = $1 \
                 WHERE projection_name = $2 AND status = 'building'"
            )))
            .bind(event.sequence)
            .bind(&rebuild.projection.name)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
    }

    for rebuild in &building_rebuilds {
        let current = get_projection_rebuild(
            pool,
            bounded_context,
            &rebuild.projection.name,
            ProjectionRebuildStatus::Building,
        )
        .await?
        .expect("a building rebuild this function just loaded can't have vanished mid-tick");
        if current.caught_up_to.unwrap_or(-1) == latest {
            // A cheap fast-path filter, not the authoritative check
            // anymore (drift audit finding #6) - `promote_projection_rebuild`
            // re-verifies for real under its own lock, so a `false`
            // return here is an expected, self-correcting outcome (a new
            // event landed in the gap between this snapshot and that
            // lock) rather than something this caller needs to react to;
            // the next tick retries.
            let _ =
                promote_projection_rebuild(pool, bounded_context, &rebuild.projection.name).await?;
        }
    }

    // Codeberg issue #25 (docs/architecture.md §51) - sequential across
    // partitioned projections, not `for_each_concurrent`: the real
    // scale-out value comes from separate *instances* racing for
    // partitions, not from one instance's own intra-tick fan-out, and
    // stacking more concurrency here on top of this function's own
    // already-concurrent per-bounded-context caller
    // (`BACKGROUND_TASK_CONCURRENCY` in `skilj/src/lib.rs`) against a
    // pool whose default cap is modest (docs/architecture.md §35) risks
    // exhausting it for little gain.
    for (projection, partition_count) in partitioned_async_projections {
        catch_up_partitioned_projection(
            pool,
            bounded_context,
            &schema,
            dispatcher,
            projection,
            partition_count,
            latest,
        )
        .await?;
    }

    Ok(())
}

/// `catch_up_bounded_context`'s own per-tick body for one async
/// `Projection` whose `Projection::PARTITION_COUNT > 1` (Codeberg issue
/// #25's investigation, docs/architecture.md §50/§51 - see that
/// section for the full design writeup). Reuses `keys()` itself as the
/// partitioning input - no separate "partition key" concept - hashing
/// each key `dispatcher.keys()` returns into one of `partition_count`
/// buckets via `partition_for_key` (a hand-written, cross-version-stable
/// FNV-1a, not `std::collections::hash_map::DefaultHasher`, which is
/// explicitly not guaranteed stable across Rust versions and would risk
/// two instances silently disagreeing on a key's own bucket).
///
/// Claiming is a non-blocking, per-tick race for each partition's own
/// `pg_try_advisory_xact_lock`, the identical primitive
/// `migrate_idempotency_keys_client_id_scoping` already establishes -
/// whichever instance wins a given partition this tick folds every
/// pending key in that bucket, inside the one transaction that holds
/// the lock; a losing instance simply skips that partition this tick,
/// with no lease/heartbeat/expiry machinery needed since a dead
/// instance just stops winning locks. **This lock is a pure
/// work-avoidance optimization, not a correctness mechanism**: every
/// fold below still goes through the exact same
/// `get_or_create_projection_state_for_update` → `as_of_sequence` guard
/// → `dispatcher.project()` → `apply_projection_fold_update` sequence
/// the unpartitioned loop above uses, so even a hash disagreement
/// between instances (a theoretical `hashtext` collision, or a rolling
/// deploy briefly running two different `PARTITION_COUNT` values) could
/// only ever cause wasted duplicate work, never a double-fold.
///
/// One transaction per `(partition, tick)`, not per-event: a
/// non-blocking lock re-acquired per event would force stopping at the
/// first lost race anyway, since a partition's own progress can only
/// advance contiguously in sequence order - far more lock round trips
/// for no real benefit. The real cost this trades away, relative to the
/// unpartitioned loop's own per-event transaction granularity, is
/// durability: a poison event anywhere in a partition's own batch this
/// tick rolls back that whole partition's tick, not just the offending
/// event - `MAX_EVENTS_PER_PARTITION_TICK` bounds the blast radius, and
/// the next tick's fresh, non-blocking race is self-healing regardless
/// of which instance (if any) previously held that partition.
///
/// Deliberately excludes a `ProjectionRebuild`'s own `building` fold and
/// `fold_history_into_new_sync_projection` - both stay single-instance,
/// exactly as before `PARTITION_COUNT` existed. Rebuilds are one-time,
/// bounded events (only `schema_changed`/`consumed_change_has_history`/
/// `becoming_sync` in `register_projection` ever stage one), not the
/// sustained-throughput concern issue #25 is actually about.
///
/// **Known accepted limitation**: after a rebuild promotes
/// (`promote_projection_rebuild`), this projection's own
/// `projection_partition_progress` rows can be stale relative to the
/// freshly-jumped `projections.caught_up_to` the rebuild just set - the
/// next tick(s) will re-scan the gap (capped, self-healing over a few
/// ticks by `MAX_EVENTS_PER_PARTITION_TICK`) purely as wasted work,
/// never a correctness risk, since `as_of_sequence` still skips every
/// already-folded key/event pair it re-scans. Not worth promotion-side
/// bookkeeping to close for what is a rare, self-correcting cost.
#[allow(clippy::too_many_arguments)]
async fn catch_up_partitioned_projection(
    pool: &Pool,
    bounded_context: &str,
    schema: &str,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    projection: &Projection,
    partition_count: u32,
    latest: i64,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_partition_progress \
         (projection_name, partition_index, caught_up_to, updated_at) \
         SELECT $1, gs, -1, now() FROM generate_series(0, $2::int - 1) AS gs \
         ON CONFLICT (projection_name, partition_index) DO NOTHING"
    )))
    .bind(&projection.name)
    .bind(partition_count as i32)
    .execute(pool)
    .await?;

    let mut progress = list_projection_partition_progress(pool, schema, &projection.name).await?;
    let min_caught_up = (0..partition_count)
        .map(|p| progress.get(&p).copied().unwrap_or(-1))
        .min()
        .unwrap_or(latest);

    let events = if min_caught_up >= latest {
        Vec::new()
    } else {
        list_events_for_bounded_context_from_limited(
            pool,
            bounded_context,
            min_caught_up,
            MAX_EVENTS_PER_PARTITION_TICK,
        )
        .await?
    };

    if !events.is_empty() {
        let batch_end = events
            .last()
            .expect("just checked events is non-empty")
            .sequence;
        let default_state_json = dispatcher
            .default_state(bounded_context, &projection.name)
            .unwrap_or_default();
        let owner_tag_key = dispatcher
            .owner_tag_key(bounded_context, &projection.name)
            .flatten();

        for partition_index in 0..partition_count {
            if progress.get(&partition_index).copied().unwrap_or(-1) >= batch_end {
                continue;
            }

            let mut tx = pool.begin().await?;
            let locked: bool =
                sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1)::bigint)")
                    .bind(partition_lock_key(
                        bounded_context,
                        &projection.name,
                        partition_index,
                    ))
                    .fetch_one(&mut *tx)
                    .await?;
            if !locked {
                // Nothing was written under this transaction - dropping
                // it is a no-op rollback, not a partial commit to worry
                // about. Another instance (or this one, next tick) owns
                // this partition for now.
                continue;
            }

            let mut this_partition_progress = progress.get(&partition_index).copied().unwrap_or(-1);
            for event in &events {
                if this_partition_progress >= event.sequence {
                    continue;
                }
                let keys = dispatcher
                    .keys(bounded_context, &projection.name, event)
                    .unwrap_or_default();
                for key in &keys {
                    if partition_for_key(key, partition_count) != partition_index {
                        continue;
                    }

                    // Identical guard to the unpartitioned loop above -
                    // see its own comment. Here it also absorbs any
                    // hash disagreement between instances (this
                    // function's own doc comment's "pure work-avoidance
                    // optimization" point).
                    let (as_of_sequence, current_state) =
                        get_or_create_projection_state_for_update(
                            &mut *tx,
                            schema,
                            &projection.name,
                            key,
                            &default_state_json,
                        )
                        .await?;
                    if as_of_sequence >= event.sequence {
                        continue;
                    }

                    let new_state = match dispatcher.project(
                        bounded_context,
                        &projection.name,
                        &current_state,
                        event,
                        key,
                    ) {
                        Some(result) => result?,
                        None => current_state,
                    };

                    apply_projection_fold_update(
                        &mut *tx,
                        schema,
                        "projection_state",
                        "",
                        &projection.name,
                        key,
                        &new_state,
                        owner_tag_key,
                        event,
                    )
                    .await?;
                }
                this_partition_progress = event.sequence;
            }

            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {schema}.projection_partition_progress \
                 (projection_name, partition_index, caught_up_to, updated_at) \
                 VALUES ($1, $2, $3, now()) \
                 ON CONFLICT (projection_name, partition_index) DO UPDATE SET \
                    caught_up_to = GREATEST(\
                        {schema}.projection_partition_progress.caught_up_to, EXCLUDED.caught_up_to), \
                    updated_at = now()"
            )))
            .bind(&projection.name)
            .bind(partition_index as i32)
            .bind(this_partition_progress)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            progress.insert(partition_index, this_partition_progress);
        }
    }

    // Rolled up regardless of which partitions (if any) this instance
    // won this tick - any instance computing this is harmless, idempotent
    // work, and it's what keeps `projections.caught_up_to` (the single
    // external source of truth GraphQL's `caughtUpTo`/`wait_until_caught_up`
    // depend on) meaning exactly what it always has: every key of this
    // projection reflects every event up to this sequence. The
    // `partition_index < $2` filter excludes any row left over from a
    // since-decreased `PARTITION_COUNT`, so it can never wedge the
    // rollup on a stale high-index row nothing advances anymore.
    let rolled_up: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT MIN(caught_up_to) FROM {schema}.projection_partition_progress \
         WHERE projection_name = $1 AND partition_index < $2"
    )))
    .bind(&projection.name)
    .bind(partition_count as i32)
    .fetch_one(pool)
    .await?;
    if let Some(rolled_up) = rolled_up {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.projections SET caught_up_to = $1 \
             WHERE name = $2 AND (caught_up_to IS NULL OR caught_up_to < $1)"
        )))
        .bind(rolled_up)
        .bind(&projection.name)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// A generous bound on how many events one partitioned projection's own
/// per-partition-per-tick transaction (`catch_up_partitioned_projection`)
/// spans - not a correctness requirement, but for a different reason
/// than `MAX_DUE_DEADLINES_PER_TICK`/`MAX_OCCURRENCES_PER_TICK`'s own
/// "don't starve other work sharing this tick": a partitioned
/// projection's own tick is a *single all-or-nothing transaction* per
/// partition (see `catch_up_partitioned_projection`'s own doc comment
/// for why), so without a cap, a poison event anywhere in a truly
/// enormous post-outage backlog would roll back that entire backlog's
/// worth of progress for that partition, not just the offending event -
/// the unpartitioned path's own per-event transaction granularity has no
/// such exposure. Events beyond this cap are simply left for the next
/// tick, which resumes from wherever this partition's own progress
/// landed - the same "recent window, not a hard limit on correctness"
/// register those two constants are already in.
const MAX_EVENTS_PER_PARTITION_TICK: i64 = 1000;

/// `catch_up_partitioned_projection`'s own key→partition assignment -
/// hand-written 64-bit FNV-1a rather than
/// `std::collections::hash_map::DefaultHasher` (explicitly not
/// guaranteed stable across Rust versions/std/build flags - unsuitable
/// for a scheme that must agree across every instance in a fleet,
/// possibly running slightly different builds during a rolling deploy)
/// or a new crate dependency (not judged worth it for a few lines of
/// well-known, public-domain algorithm). Computed in Rust, not pushed
/// into Postgres via `hashtext()`, since partition membership must be
/// known *before* deciding which partitions are even worth a lock
/// attempt this tick - `hashtext()` is still used, unrelated, for the
/// advisory lock key itself (`partition_lock_key` below).
fn partition_for_key(key: &str, partition_count: u32) -> u32 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash % u64::from(partition_count)) as u32
}

/// The `pg_try_advisory_xact_lock` key `catch_up_partitioned_projection`
/// races for one `(bounded_context, projection, partition)` triple -
/// bound via `hashtext($1)::bigint`, the same pattern
/// `migrate_idempotency_keys_client_id_scoping` already establishes. A
/// `hashtext` collision with an unrelated lock key is theoretically
/// possible (32-bit output) but harmless here: it would only ever cause
/// a spurious, self-healing missed attempt this tick - exactly why this
/// function's own caller uses the non-blocking `pg_try_advisory_xact_lock`
/// form rather than the blocking `pg_advisory_xact_lock` the migration
/// guard uses.
fn partition_lock_key(
    bounded_context: &str,
    projection_name: &str,
    partition_index: u32,
) -> String {
    format!("projection_partition:{bounded_context}:{projection_name}:{partition_index}")
}

/// `catch_up_partitioned_projection`'s own progress read - every
/// `projection_partition_progress` row for one projection, as a
/// `partition_index -> caught_up_to` map. A partition index with no row
/// yet (shouldn't happen once that function's own seed step has run,
/// but not assumed) is simply absent from the map - every caller already
/// treats a missing entry as `-1` via `.unwrap_or(-1)`, the same
/// "nothing folded yet" convention `projection_state.as_of_sequence`'s
/// own default uses.
async fn list_projection_partition_progress(
    pool: &Pool,
    schema: &str,
    projection_name: &str,
) -> crate::error::Result<std::collections::HashMap<u32, i64>> {
    let rows: Vec<(i32, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT partition_index, caught_up_to \
         FROM {schema}.projection_partition_progress WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(idx, seq)| (idx as u32, seq))
        .collect())
}

/// [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 2" - the background half of
/// `Snapshot`: one poll tick, for one bounded context, folding every
/// committed event not yet reflected in any registered snapshot.
/// Deliberately its own function, not folded into `catch_up_bounded_context`
/// above even though the shape closely mirrors it - see `Snapshot`'s own
/// doc comment for why the two stay structurally separate. Called in a
/// loop, on a timer, by its own shared background task
/// `SkiljBuilder::build()` spawns.
///
/// Unlike `Projection` (discovered via the `projections` metadata
/// table), which snapshots exist is asked of `dispatcher` directly -
/// `SnapshotDispatcher::snapshot_names` - since there is deliberately no
/// metadata table for `Snapshot` (see that trait's own doc comment).
/// Progress is tracked per `snapshot_name` in `snapshot_progress`
/// (`Projection`'s own `caught_up_to` column isn't reused, for the same
/// separation reason), and, per event, at most one tag *value*'s own row
/// is touched - `Snapshot::TAG_KEY` names a single tag key, so `event.tags`
/// either does or doesn't carry it, no `keys()`-equivalent fan-out the
/// way a multi-instance `Projection` needs.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn catch_up_snapshots(
    pool: &Pool,
    bounded_context: &str,
    dispatcher: &dyn crate::plugin::SnapshotDispatcher,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let latest = latest_sequence(pool, bounded_context).await?.unwrap_or(-1);

    let all_snapshot_names = dispatcher.snapshot_names(bounded_context);
    // Codeberg issue #25 (docs/architecture.md §52) - split by
    // `Snapshot::PARTITION_COUNT` (via the dispatcher, the only place
    // this Rust-only config lives, exactly like `Projection`'s own
    // split in `catch_up_bounded_context`). `unpartitioned_snapshot_names`
    // (`<= 1`, the default, and every snapshot that predates this
    // feature) feeds the exact same loop this function has always had,
    // completely unchanged below.
    let unpartitioned_snapshot_names: Vec<&str> = all_snapshot_names
        .iter()
        .filter(|name| {
            dispatcher
                .partition_count(bounded_context, name)
                .unwrap_or(1)
                .max(1)
                <= 1
        })
        .copied()
        .collect();
    let partitioned_snapshot_names: Vec<(&str, u32)> = all_snapshot_names
        .iter()
        .filter_map(|name| {
            let partition_count = dispatcher
                .partition_count(bounded_context, name)
                .unwrap_or(1)
                .max(1);
            (partition_count > 1).then_some((*name, partition_count))
        })
        .collect();
    if unpartitioned_snapshot_names.is_empty() && partitioned_snapshot_names.is_empty() {
        return Ok(());
    }

    let progress_from_db =
        list_snapshot_progress_for_bounded_context(pool, bounded_context).await?;
    let mut progress: std::collections::HashMap<&str, i64> = unpartitioned_snapshot_names
        .iter()
        .map(|name| (*name, progress_from_db.get(*name).copied().unwrap_or(-1)))
        .collect();

    let min_caught_up = progress.values().copied().min().unwrap_or(latest);
    let events = if unpartitioned_snapshot_names.is_empty() || min_caught_up >= latest {
        Vec::new()
    } else {
        list_events_for_bounded_context_from(pool, bounded_context, min_caught_up).await?
    };

    for event in &events {
        let mut tx = pool.begin().await?;

        for name in &unpartitioned_snapshot_names {
            if progress[name] >= event.sequence {
                continue;
            }

            // `None` (not registered) can't actually happen here -
            // `name` came from this exact dispatcher's own
            // `snapshot_names` a moment ago - but treated as "nothing to
            // do" rather than unwrapped, the same defensive posture
            // `catch_up_bounded_context` already takes for its own
            // dispatcher lookups.
            let Some(tag_key) = dispatcher.tag_key(bounded_context, name) else {
                continue;
            };
            let Some(tag) = event.tags.iter().find(|t| t.key == tag_key) else {
                continue;
            };
            let Some(tag_value) = &tag.value else {
                continue;
            };
            let version = dispatcher.version(bounded_context, name).unwrap_or(0);
            let default_state_json = dispatcher
                .default_state(bounded_context, name)
                .unwrap_or_default();

            let (as_of_sequence, current_state) = get_or_create_snapshot_state_for_update(
                &mut *tx,
                &schema,
                name,
                tag_key,
                tag_value,
                version,
                &default_state_json,
            )
            .await?;

            if as_of_sequence >= event.sequence {
                continue;
            }

            let new_state = match dispatcher.fold(bounded_context, name, &current_state, event) {
                Some(result) => result?,
                None => current_state,
            };

            // Cross-tenant read fix (docs/architecture.md's own
            // write-up of these passes) - `apply_projection_fold_update`'s
            // own identical reasoning, for `Snapshot::OWNER_TAG_KEY`
            // instead of `Projection::OWNER_TAG_KEY`: this event's own
            // tag under that key (not necessarily `tag_key` itself)
            // becomes this row's own derived `owner`, when present. An
            // event lacking it leaves an already-established owner
            // untouched, so `owner` is only ever included in the SET
            // list when this event actually supplies one.
            let owner = dispatcher
                .owner_tag_key(bounded_context, name)
                .flatten()
                .and_then(|owner_tag_key| {
                    event
                        .tags
                        .iter()
                        .find(|t| t.key == owner_tag_key)
                        .and_then(|t| t.value.clone())
                });

            match owner {
                Some(owner) => {
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "UPDATE {schema}.snapshots SET snapshot_version = $1, as_of_sequence = $2, \
                         state = $3::jsonb, owner = $4, updated_at = now() \
                         WHERE snapshot_name = $5 AND tag_key = $6 AND tag_value = $7"
                    )))
                    .bind(version as i64)
                    .bind(event.sequence)
                    .bind(&new_state)
                    .bind(owner)
                    .bind(name)
                    .bind(tag_key)
                    .bind(tag_value.as_str())
                    .execute(&mut *tx)
                    .await?;
                }
                None => {
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "UPDATE {schema}.snapshots SET snapshot_version = $1, as_of_sequence = $2, \
                         state = $3::jsonb, updated_at = now() \
                         WHERE snapshot_name = $4 AND tag_key = $5 AND tag_value = $6"
                    )))
                    .bind(version as i64)
                    .bind(event.sequence)
                    .bind(&new_state)
                    .bind(name)
                    .bind(tag_key)
                    .bind(tag_value.as_str())
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }

        for name in &unpartitioned_snapshot_names {
            if progress[name] >= event.sequence {
                continue;
            }
            upsert_snapshot_progress(&mut *tx, &schema, name, event.sequence).await?;
            progress.insert(*name, event.sequence);
        }

        tx.commit().await?;
    }

    // Codeberg issue #25 (docs/architecture.md §52) - sequential across
    // partitioned snapshots, not `for_each_concurrent`, the identical
    // reasoning `catch_up_bounded_context`'s own partitioned-projection
    // loop already documents (§51): the real scale-out value comes from
    // separate *instances* racing for partitions, not from one
    // instance's own intra-tick fan-out.
    for (name, partition_count) in partitioned_snapshot_names {
        catch_up_partitioned_snapshot(
            pool,
            bounded_context,
            &schema,
            dispatcher,
            name,
            partition_count,
            latest,
        )
        .await?;
    }

    Ok(())
}

/// `catch_up_snapshots`'s own per-tick body for one `Snapshot` whose
/// `Snapshot::PARTITION_COUNT > 1` (Codeberg issue #25, docs/architecture.md
/// §52 - the `Snapshot` twin of `catch_up_partitioned_projection`, see
/// that function's own doc comment for the full design writeup, not
/// repeated in full here). The one structural difference: `Snapshot`
/// derives at most *one* tag value per event (there's no `keys()`-style
/// fan-out - see `Snapshot`'s own doc comment), so partitioning hashes
/// that single derived tag value instead of iterating several keys per
/// event.
///
/// Same primitives throughout: `partition_for_key` (the identical
/// FNV-1a §51 established), a non-blocking
/// `pg_try_advisory_xact_lock` per `(snapshot_name, partition)` per
/// tick (`snapshot_partition_lock_key`), one transaction per
/// `(partition, tick)`, `MAX_EVENTS_PER_PARTITION_TICK`-capped batches
/// via the existing `list_events_for_bounded_context_from_limited`, and
/// a monotonic-only rollup into `snapshot_progress.caught_up_to` (the
/// single external source of truth, unchanged in shape - `Snapshot` has
/// no GraphQL-exposed `caughtUpTo` field the way `Projection` does, but
/// `resolve_snapshot_context`'s own correctness already depends on
/// `snapshots.as_of_sequence` per row, untouched by any of this).
#[allow(clippy::too_many_arguments)]
async fn catch_up_partitioned_snapshot(
    pool: &Pool,
    bounded_context: &str,
    schema: &str,
    dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    name: &str,
    partition_count: u32,
    latest: i64,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.snapshot_partition_progress \
         (snapshot_name, partition_index, caught_up_to, updated_at) \
         SELECT $1, gs, -1, now() FROM generate_series(0, $2::int - 1) AS gs \
         ON CONFLICT (snapshot_name, partition_index) DO NOTHING"
    )))
    .bind(name)
    .bind(partition_count as i32)
    .execute(pool)
    .await?;

    let mut progress = list_snapshot_partition_progress(pool, schema, name).await?;
    let min_caught_up = (0..partition_count)
        .map(|p| progress.get(&p).copied().unwrap_or(-1))
        .min()
        .unwrap_or(latest);

    let events = if min_caught_up >= latest {
        Vec::new()
    } else {
        list_events_for_bounded_context_from_limited(
            pool,
            bounded_context,
            min_caught_up,
            MAX_EVENTS_PER_PARTITION_TICK,
        )
        .await?
    };

    if !events.is_empty() {
        let batch_end = events
            .last()
            .expect("just checked events is non-empty")
            .sequence;
        let Some(tag_key) = dispatcher.tag_key(bounded_context, name) else {
            return Ok(());
        };
        let version = dispatcher.version(bounded_context, name).unwrap_or(0);
        let default_state_json = dispatcher
            .default_state(bounded_context, name)
            .unwrap_or_default();
        let owner_tag_key = dispatcher.owner_tag_key(bounded_context, name).flatten();

        for partition_index in 0..partition_count {
            if progress.get(&partition_index).copied().unwrap_or(-1) >= batch_end {
                continue;
            }

            let mut tx = pool.begin().await?;
            let locked: bool =
                sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1)::bigint)")
                    .bind(snapshot_partition_lock_key(
                        bounded_context,
                        name,
                        partition_index,
                    ))
                    .fetch_one(&mut *tx)
                    .await?;
            if !locked {
                // Nothing was written under this transaction - dropping
                // it is a no-op rollback. Another instance (or this
                // one, next tick) owns this partition for now.
                continue;
            }

            let mut this_partition_progress = progress.get(&partition_index).copied().unwrap_or(-1);
            for event in &events {
                if this_partition_progress >= event.sequence {
                    continue;
                }
                let tag_value = event
                    .tags
                    .iter()
                    .find(|t| t.key == tag_key)
                    .and_then(|t| t.value.as_deref());
                if let Some(tag_value) = tag_value {
                    if partition_for_key(tag_value, partition_count) == partition_index {
                        let (as_of_sequence, current_state) =
                            get_or_create_snapshot_state_for_update(
                                &mut *tx,
                                schema,
                                name,
                                tag_key,
                                tag_value,
                                version,
                                &default_state_json,
                            )
                            .await?;
                        // Identical guard to the unpartitioned loop
                        // above - see `catch_up_partitioned_projection`'s
                        // own comment for why this is what actually
                        // makes cross-instance racing safe, the
                        // advisory lock being only an optimization.
                        if as_of_sequence < event.sequence {
                            let new_state =
                                match dispatcher.fold(bounded_context, name, &current_state, event)
                                {
                                    Some(result) => result?,
                                    None => current_state,
                                };
                            let owner = owner_tag_key.and_then(|owner_tag_key| {
                                event
                                    .tags
                                    .iter()
                                    .find(|t| t.key == owner_tag_key)
                                    .and_then(|t| t.value.clone())
                            });
                            match owner {
                                Some(owner) => {
                                    sqlx::query(sqlx::AssertSqlSafe(format!(
                                        "UPDATE {schema}.snapshots SET snapshot_version = $1, \
                                         as_of_sequence = $2, state = $3::jsonb, owner = $4, \
                                         updated_at = now() \
                                         WHERE snapshot_name = $5 AND tag_key = $6 AND tag_value = $7"
                                    )))
                                    .bind(version as i64)
                                    .bind(event.sequence)
                                    .bind(&new_state)
                                    .bind(owner)
                                    .bind(name)
                                    .bind(tag_key)
                                    .bind(tag_value)
                                    .execute(&mut *tx)
                                    .await?;
                                }
                                None => {
                                    sqlx::query(sqlx::AssertSqlSafe(format!(
                                        "UPDATE {schema}.snapshots SET snapshot_version = $1, \
                                         as_of_sequence = $2, state = $3::jsonb, updated_at = now() \
                                         WHERE snapshot_name = $4 AND tag_key = $5 AND tag_value = $6"
                                    )))
                                    .bind(version as i64)
                                    .bind(event.sequence)
                                    .bind(&new_state)
                                    .bind(name)
                                    .bind(tag_key)
                                    .bind(tag_value)
                                    .execute(&mut *tx)
                                    .await?;
                                }
                            }
                        }
                    }
                }
                this_partition_progress = event.sequence;
            }

            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {schema}.snapshot_partition_progress \
                 (snapshot_name, partition_index, caught_up_to, updated_at) \
                 VALUES ($1, $2, $3, now()) \
                 ON CONFLICT (snapshot_name, partition_index) DO UPDATE SET \
                    caught_up_to = GREATEST(\
                        {schema}.snapshot_partition_progress.caught_up_to, EXCLUDED.caught_up_to), \
                    updated_at = now()"
            )))
            .bind(name)
            .bind(partition_index as i32)
            .bind(this_partition_progress)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            progress.insert(partition_index, this_partition_progress);
        }
    }

    // Rolled up regardless of which partitions (if any) this instance
    // won this tick - see `catch_up_partitioned_projection`'s own
    // identical comment. `partition_index < $2` excludes any row left
    // over from a since-decreased `PARTITION_COUNT`.
    let rolled_up: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT MIN(caught_up_to) FROM {schema}.snapshot_partition_progress \
         WHERE snapshot_name = $1 AND partition_index < $2"
    )))
    .bind(name)
    .bind(partition_count as i32)
    .fetch_one(pool)
    .await?;
    if let Some(rolled_up) = rolled_up {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.snapshot_progress (snapshot_name, caught_up_to) \
             VALUES ($1, $2) \
             ON CONFLICT (snapshot_name) DO UPDATE SET \
                caught_up_to = GREATEST({schema}.snapshot_progress.caught_up_to, EXCLUDED.caught_up_to)"
        )))
        .bind(name)
        .bind(rolled_up)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// `catch_up_partitioned_snapshot`'s own lock key - `partition_lock_key`'s
/// identical `hashtext(...)::bigint` pattern, namespaced by `"snapshot_partition"`
/// rather than `"projection_partition"` so the two families can never
/// collide with each other even if a projection and a snapshot happened
/// to share a name.
fn snapshot_partition_lock_key(
    bounded_context: &str,
    snapshot_name: &str,
    partition_index: u32,
) -> String {
    format!("snapshot_partition:{bounded_context}:{snapshot_name}:{partition_index}")
}

/// `catch_up_partitioned_snapshot`'s own progress read - `list_projection_partition_progress`'s
/// identical shape, for `snapshot_partition_progress` instead.
async fn list_snapshot_partition_progress(
    pool: &Pool,
    schema: &str,
    snapshot_name: &str,
) -> crate::error::Result<std::collections::HashMap<u32, i64>> {
    let rows: Vec<(i32, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT partition_index, caught_up_to \
         FROM {schema}.snapshot_partition_progress WHERE snapshot_name = $1"
    )))
    .bind(snapshot_name)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(idx, seq)| (idx as u32, seq))
        .collect())
}

/// `RegisterProjection`'s own synchronous counterpart to
/// `catch_up_bounded_context`'s async walk - drift audit finding #3
/// (2026-08-20, see project memory `skilj-drift-audit-2026-08-20` and
/// `ProjectionRegistration::Created`'s own `needs_history_fold` doc
/// comment). Called exactly once, immediately after `upsert_projection`,
/// whenever `register_projection` reports `needs_history_fold: true` -
/// never for a re-registration (that path already goes through
/// `RebuildStaged`/`promote_projection_rebuild` instead) and never for an
/// async projection (`catch_up_bounded_context`'s own periodic walk
/// already backfills those correctly, `caught_up_to.unwrap_or(-1)`
/// treating a freshly created row exactly like a stale one).
///
/// Walks every event the bounded context has ever committed, not just
/// ones matching `projection.consumed_event_types` - the same "let the
/// dispatcher decide" shape `catch_up_bounded_context` itself uses, so
/// this can never disagree with what the async path would have produced
/// had the projection been async instead. One transaction per event,
/// same as `catch_up_bounded_context`'s own per-event loop; this only
/// ever runs once, at first-time registration, so the cost is paid once
/// per projection, not on every poll tick.
///
/// Returns the given `Projection` with `caught_up_to` set to the highest
/// sequence folded - always `Some` when this is called at all, since the
/// caller only calls it when `needs_history_fold` was true, which itself
/// requires at least one matching event to exist.
#[tracing::instrument(skip_all)]
pub async fn fold_history_into_new_sync_projection(
    pool: &Pool,
    projection: &Projection,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
) -> crate::error::Result<Projection> {
    let bounded_context = &projection.bounded_context.name;
    let schema = schema_ident(bounded_context);
    let events = list_events_for_bounded_context(pool, bounded_context).await?;
    let default_state_json = dispatcher
        .default_state(bounded_context, &projection.name)
        .unwrap_or_default();
    let owner_tag_key = dispatcher
        .owner_tag_key(bounded_context, &projection.name)
        .flatten();

    let mut caught_up_to = None;
    for event in &events {
        let mut tx = pool.begin().await?;

        let keys = dispatcher
            .keys(bounded_context, &projection.name, event)
            .unwrap_or_default();
        for key in &keys {
            // `as_of_sequence` guard - see `catch_up_bounded_context`'s
            // identical comment. This function's own race is different in
            // shape (two instances both reconciling the *same brand-new*
            // registration concurrently - `register_projection`'s own
            // `existing = None` read-then-decide has no claim mechanism,
            // so both would call this function at once) but the same
            // per-row fix closes it: whichever instance's transaction
            // commits a key's row first, the other's own `RETURNING`
            // here sees `as_of_sequence` already at `event.sequence` and
            // skips instead of folding again.
            let (as_of_sequence, current_state) = get_or_create_projection_state_for_update(
                &mut *tx,
                &schema,
                &projection.name,
                key,
                &default_state_json,
            )
            .await?;
            if as_of_sequence >= event.sequence {
                continue;
            }

            let new_state = match dispatcher.project(
                bounded_context,
                &projection.name,
                &current_state,
                event,
                key,
            ) {
                Some(result) => result?,
                None => current_state,
            };

            apply_projection_fold_update(
                &mut *tx,
                &schema,
                "projection_state",
                "",
                &projection.name,
                key,
                &new_state,
                owner_tag_key,
                event,
            )
            .await?;
        }

        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.projections SET caught_up_to = $1 WHERE name = $2"
        )))
        .bind(event.sequence)
        .bind(&projection.name)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        caught_up_to = Some(event.sequence);
    }

    Ok(Projection {
        caught_up_to,
        ..projection.clone()
    })
}

/// Promotes a `building` `ProjectionRebuild` that has caught up to its
/// bounded context's latest committed sequence - `catch_up_bounded_context`'s
/// own last step for each rebuild it walks. One transaction: the rebuild's
/// `schema`/`schema_version`/`sync`/`caught_up_to` replace the live
/// `Projection` row's own, `projection_consumed_event_types` is replaced
/// wholesale from `projection_rebuild_consumed_event_types`, and
/// `projection_rebuild_state`'s content replaces `projection_state`'s
/// (the live projection's old state is discarded - the note above
/// `RegisterProjection`) - then every rebuild-side row is deleted. A
/// rebuild the dispatcher never once resolved (see `catch_up_bounded_context`'s
/// own doc comment) still promotes on schedule; its `projection_rebuild_state`
/// row, if one was ever written, is copied as-is - `"{}"` included.
///
/// Drift audit finding #6 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): the caller's own `caught_up_to ==
/// latest` pre-check is only a snapshot taken before this call, not a
/// lock - without one, a new event could commit in the gap between that
/// check and this function's own writes, invisible to both sides
/// forever after (never folded into the rebuild, which this promotes
/// away from `building` before it can be; never folded inline either,
/// since the live projection was still `sync = false` when it arrived).
/// Closed the same way `submit_command`'s own DCB re-check is: locking
/// the bounded context's `{schema}.sequence` row first
/// (`SELECT ... FOR UPDATE`, the identical query, since `next_value`
/// already *is* the latest committed sequence per
/// `SequenceIsGaplessPerBoundedContext` - no separate lookup needed),
/// then re-verifying eligibility under that lock rather than trusting
/// the pre-lock snapshot. This is also what makes
/// `insert_event_and_update_sync_projections_in_tx`'s own
/// `sync_projections` read safe to leave running on the bare pool,
/// outside its own `tx` - see that function's doc comment: every
/// event-insert path already takes this exact lock before reaching that
/// read, so once promotion takes it too, the two can never interleave.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn promote_projection_rebuild(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
) -> crate::error::Result<bool> {
    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;

    // The lock this whole fix hinges on - see this function's own doc
    // comment. A read-only peek, not `next_sequence`'s increment: this
    // never allocates a sequence number of its own.
    let (locked_highest,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT next_value FROM {schema}.sequence FOR UPDATE"
    )))
    .fetch_one(&mut *tx)
    .await?;

    // Every rebuild-side statement below is scoped to `status = 'building'`
    // - promotion only ever ends the building row. A coexisting pending
    // row (the deliberate case `UniqueRebuildPerProjectionAndStatus`
    // names - a non-trivial registration that arrived mid-build) must
    // survive this call untouched: it is still waiting on its own future
    // `RebuildProjection` trigger, unrelated to whichever build just
    // finished.
    let building = projection_rebuild_status_to_str(ProjectionRebuildStatus::Building);

    let rebuild_row: ProjectionRebuildRow = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_REBUILD_COLUMNS} FROM {schema}.projection_rebuilds \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .fetch_one(&mut *tx)
    .await?;

    if rebuild_row.caught_up_to.unwrap_or(-1) != locked_highest {
        // A new event committed after this bounded context's last fold
        // pass but before this lock was acquired - promoting now would
        // strand it. Defer: the next catch_up_bounded_context tick folds
        // it into this still-building rebuild and retries promotion
        // then. `tx` drops here, rolling back the no-op peek lock -
        // nothing was written, nothing to undo.
        return Ok(false);
    }

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.projections SET schema = $1, schema_version = $2, sync = $3, \
         caught_up_to = $4 WHERE name = $5"
    )))
    .bind(&rebuild_row.schema)
    .bind(rebuild_row.schema_version)
    .bind(rebuild_row.sync)
    .bind(rebuild_row.caught_up_to)
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_consumed_event_types WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_consumed_event_types (projection_name, event_type_name) \
         SELECT projection_name, event_type_name \
         FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;

    // Every instance the rebuild accumulated replaces every live one -
    // not just one row anymore (§9's "keyed / multi-row Projections"
    // pass): delete the live set for this projection wholesale, then
    // copy the rebuild's own set across in its place, the identical
    // "delete then bulk `INSERT ... SELECT`" shape
    // `projection_consumed_event_types` just above already uses. A
    // rebuild whose dispatcher never resolved even once (see this
    // function's own doc comment) has no rebuild-side rows to copy at
    // all - the live set is simply left empty, not clobbered with
    // nothing pretending to be something.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_state WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;
    // `owner` carried across too - a promoted rebuild keeps whatever
    // ownership it derived while building, exactly as `state` does
    // (cross-tenant projection read fix, docs/architecture.md's own
    // write-up of this pass). `as_of_sequence` carried across for the
    // identical reason (Codeberg issue #25's investigation finding) -
    // and load-bearing here, not just consistency: leaving it out would
    // have every promoted row's `as_of_sequence` default back to `-1`
    // while `state` already reflects the rebuild's full replay, so the
    // very next `catch_up_bounded_context` tick would see "never folded"
    // and replay every historical event into already-folded state a
    // second time - the exact bug this whole pass fixes, reintroduced
    // right here if this column were dropped from the copy.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_state \
         (projection_name, key, state, owner, as_of_sequence, updated_at) \
         SELECT projection_name, key, state, owner, as_of_sequence, updated_at \
         FROM {schema}.projection_rebuild_state WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_state WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuilds WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}

// --- AccessToken (three of its four variants - see this module's own doc comment) ---

/// `AccessToken.purpose`'s four values, mirroring the Rust `AccessToken`
/// sum type in `access_control` - used by `skilj-rest`'s auth layer to
/// tell "no token with this id exists at all" (401 - an unrecognised
/// credential) from "one does, but not of the variant this route needs"
/// (403 - docs/architecture.md §7.2's "presenting the wrong token
/// variant... is a 403, not a 404") without needing three separate
/// lookups to find out which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessTokenKind {
    ExternalEvent,
    DirectCreation,
    EventRead,
    Command,
}

impl AccessTokenKind {
    fn as_str(self) -> &'static str {
        match self {
            AccessTokenKind::ExternalEvent => "external_event",
            AccessTokenKind::DirectCreation => "direct_creation",
            AccessTokenKind::EventRead => "event_read",
            AccessTokenKind::Command => "command",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "direct_creation" => AccessTokenKind::DirectCreation,
            "event_read" => AccessTokenKind::EventRead,
            "command" => AccessTokenKind::Command,
            _ => AccessTokenKind::ExternalEvent,
        }
    }
}

/// The one thing a bearer credential's bare `id` can resolve on its own,
/// before anything else: which bounded context's schema actually holds
/// it. See `migrations/0001_init.sql`'s own doc comment on
/// `access_token_index` for why this index has to exist at all now that
/// `access_tokens` itself lives per-context.
async fn resolve_token_bounded_context(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT bounded_context FROM access_token_index WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(bounded_context,)| bounded_context))
}

async fn insert_token_index(
    pool: &Pool,
    id: &str,
    bounded_context: &str,
) -> crate::error::Result<()> {
    sqlx::query("INSERT INTO access_token_index (id, bounded_context) VALUES ($1, $2)")
        .bind(id)
        .bind(bounded_context)
        .execute(pool)
        .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct AccessTokenColumns {
    id: String,
    kind: String,
    secret: String,
    status: String,
    created_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
    event_type_name: Option<String>,
    command_type_name: Option<String>,
    /// One column, all four kinds - see `EventReadToken.scope`'s own doc
    /// comment for the read-side reasoning and
    /// `ExternalEventToken.scope`'s for the write-side one.
    scope: Option<String>,
    /// One column, all four kinds, meaningful only for `event_read` -
    /// see `EventReadToken.start_from`'s own doc comment.
    /// `event_read_start_position_from_str` only ever gets called on
    /// this for a genuine `event_read` row; the other three kinds carry
    /// whatever the column's own `DEFAULT` gave it, read here but never
    /// interpreted.
    start_from: String,
    /// `EventReadToken.start_at_sequence`/`.start_at_time` - two more
    /// columns, all four kinds, meaningful only for `event_read` on the
    /// identical terms `start_from` above already is.
    start_at_sequence: Option<i64>,
    start_at_time: Option<DateTime<Utc>>,
}

struct AccessTokenRow {
    columns: AccessTokenColumns,
    bounded_context: String,
}

/// Resolves the index first (see `resolve_token_bounded_context`), then
/// the token's own full row from that context's schema. `None` when
/// either step finds nothing - a bare `id` genuinely unrecognised either
/// way, the same "doesn't distinguish which" treatment
/// `RestError::UnrecognisedCredential` already gives on the wire.
async fn fetch_access_token_row(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<AccessTokenRow>> {
    let Some(bounded_context) = resolve_token_bounded_context(pool, id).await? else {
        return Ok(None);
    };
    let schema = schema_ident(&bounded_context);
    let columns: Option<AccessTokenColumns> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, kind, secret, status, created_at, revoked_at, event_type_name, \
         command_type_name, scope, start_from, start_at_sequence, start_at_time \
         FROM {schema}.access_tokens WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(columns.map(|columns| AccessTokenRow {
        columns,
        bounded_context,
    }))
}

/// See `AccessTokenKind`'s own doc comment - the one lookup `skilj-rest`'s
/// auth layer needs before deciding 401 vs 403.
#[tracing::instrument(skip_all)]
pub async fn access_token_kind(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<AccessTokenKind>> {
    Ok(fetch_access_token_row(pool, id)
        .await?
        .map(|r| AccessTokenKind::from_str(&r.columns.kind)))
}

/// Persists a `revoke_token` outcome - a status-only update, addressed by
/// `id` alone (unambiguous: `access_tokens.id` is that schema's own
/// primary key). Resolves which schema holds the row the same way
/// `fetch_access_token_row` does, via `access_token_index`, rather than
/// asking every call site to already know the bounded context -
/// `revoke_token`'s own `token` parameter carries a whole `EventType`/
/// `CommandType`, not a bare name, so re-deriving it here is simpler than
/// threading an extra parameter through. A no-op (not an error) for an
/// `id` that resolves to nothing - unreachable in practice, since the
/// caller already loaded this exact row to build the `AccessToken` it
/// passed to `revoke_token` in the first place.
#[tracing::instrument(skip_all)]
pub async fn revoke_access_token(
    pool: &Pool,
    id: &str,
    revoked_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    let Some(bounded_context) = resolve_token_bounded_context(pool, id).await? else {
        return Ok(());
    };
    let schema = schema_ident(&bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.access_tokens SET status = 'revoked', revoked_at = $1 WHERE id = $2"
    )))
    .bind(revoked_at)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// One insert shared by `insert_external_event_token`/
/// `insert_direct_creation_token`/`insert_event_read_token` below - same
/// "genuinely this wide, not a struct that exists only to satisfy the
/// lint" reasoning `event_store::process_command`'s own `#[allow]` gives.
/// Writes the token's full row into its own context's schema, then the
/// global index entry that makes it findable by `id` alone - in that
/// order, so a failure partway leaves an unreachable orphan row rather
/// than a dangling index entry pointing at nothing. `secret` is the
/// caller's plaintext (`generate_token_secret`'s own output, still held
/// in memory by the caller to hand back once) - only `hash_secret`'s
/// output of it is ever written, per `AccessToken.secret`'s own "stored
/// hashed and never compared in plaintext" text.
#[allow(clippy::too_many_arguments)]
async fn insert_access_token_row(
    pool: &Pool,
    id: &str,
    kind: AccessTokenKind,
    secret: &str,
    status: TokenStatus,
    created_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
    bounded_context: &str,
    event_type_name: &str,
    scope: Option<&str>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.access_tokens (id, kind, secret, status, created_at, revoked_at, \
         event_type_name, scope) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
    )))
    .bind(id)
    .bind(kind.as_str())
    .bind(crate::shared::hash_secret(secret))
    .bind(token_status_to_str(status))
    .bind(created_at)
    .bind(revoked_at)
    .bind(event_type_name)
    .bind(scope)
    .execute(pool)
    .await?;
    insert_token_index(pool, id, bounded_context).await
}

#[tracing::instrument(skip_all)]
pub async fn insert_external_event_token(
    pool: &Pool,
    token: &ExternalEventToken,
) -> crate::error::Result<()> {
    insert_access_token_row(
        pool,
        &token.id,
        AccessTokenKind::ExternalEvent,
        &token.secret,
        token.status,
        token.created_at,
        token.revoked_at,
        &token.event_type.bounded_context.name,
        &token.event_type.name,
        token.scope.as_deref(),
    )
    .await
}

#[tracing::instrument(skip_all)]
pub async fn insert_direct_creation_token(
    pool: &Pool,
    token: &DirectCreationToken,
) -> crate::error::Result<()> {
    insert_access_token_row(
        pool,
        &token.id,
        AccessTokenKind::DirectCreation,
        &token.secret,
        token.status,
        token.created_at,
        token.revoked_at,
        &token.event_type.bounded_context.name,
        &token.event_type.name,
        token.scope.as_deref(),
    )
    .await
}

/// Not built on `insert_access_token_row` - `EventReadToken` is the only
/// one of the three event-type-scoped kinds that also carries
/// `start_from`, so this hand-writes its own `INSERT` the same way
/// `insert_command_token` already does for its own divergent shape (see
/// that function's own doc comment).
#[tracing::instrument(skip_all)]
pub async fn insert_event_read_token(
    pool: &Pool,
    token: &EventReadToken,
) -> crate::error::Result<()> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.access_tokens (id, kind, secret, status, created_at, revoked_at, \
         event_type_name, scope, start_from, start_at_sequence, start_at_time) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)"
    )))
    .bind(&token.id)
    .bind(AccessTokenKind::EventRead.as_str())
    .bind(crate::shared::hash_secret(&token.secret))
    .bind(token_status_to_str(token.status))
    .bind(token.created_at)
    .bind(token.revoked_at)
    .bind(&token.event_type.name)
    .bind(token.scope.as_deref())
    .bind(event_read_start_position_to_str(token.start_from))
    .bind(token.start_at_sequence)
    .bind(token.start_at_time)
    .execute(pool)
    .await?;
    insert_token_index(pool, &token.id, &token.event_type.bounded_context.name).await
}

/// See `insert_access_token_row` above - not built on it directly, since
/// a `CommandToken` fills `command_type_name` instead of the
/// `event_type_name` every other variant does (see the migration's own
/// `access_tokens` `CHECK` constraint), and the same insert-row-then-index
/// ordering. `token.secret` is hashed before storage the same way
/// `insert_access_token_row` does.
#[tracing::instrument(skip_all)]
pub async fn insert_command_token(pool: &Pool, token: &CommandToken) -> crate::error::Result<()> {
    let schema = schema_ident(&token.command_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.access_tokens (id, kind, secret, status, created_at, revoked_at, \
         command_type_name, scope) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
    )))
    .bind(&token.id)
    .bind(AccessTokenKind::Command.as_str())
    .bind(crate::shared::hash_secret(&token.secret))
    .bind(token_status_to_str(token.status))
    .bind(token.created_at)
    .bind(token.revoked_at)
    .bind(&token.command_type.name)
    .bind(token.scope.as_deref())
    .execute(pool)
    .await?;
    insert_token_index(pool, &token.id, &token.command_type.bounded_context.name).await
}

/// `None` when either no token has this `id` at all, or one does but
/// isn't the expected kind - callers that need to tell those two apart
/// (for the 401-vs-403 split - see `AccessTokenKind`'s own doc comment)
/// call `access_token_kind` first. Covers `get_external_event_token`/
/// `get_direct_creation_token`, identical apart from the `kind` string
/// and return type - both resolve `event_type_name` against
/// `event_types` into an `event_type` field and carry `scope` alike
/// (cross-tenant read/write fix, docs/architecture.md's own write-up of
/// these passes). `get_event_read_token` used to share this shape too,
/// back when all three carried nothing but `scope` beyond the common
/// base - `start_from` (this pass's own write-up) broke that symmetry,
/// so it now stays hand-written below the same way `get_command_token`
/// already does for its own divergent shape.
macro_rules! get_event_type_access_token {
    ($fn_name:ident, $return_type:ident, $kind:literal) => {
        #[tracing::instrument(skip_all)]
        pub async fn $fn_name(pool: &Pool, id: &str) -> crate::error::Result<Option<$return_type>> {
            let Some(row) = fetch_access_token_row(pool, id).await? else {
                return Ok(None);
            };
            if row.columns.kind != $kind {
                return Ok(None);
            }
            let event_type_name = row
                .columns
                .event_type_name
                .expect(concat!($kind, " access_tokens row without event_type_name"));
            let event_type =
                require_event_type(pool, &row.bounded_context, &event_type_name).await?;
            Ok(Some($return_type {
                id: row.columns.id,
                secret: row.columns.secret,
                status: token_status_from_str(&row.columns.status),
                created_at: row.columns.created_at,
                revoked_at: row.columns.revoked_at,
                event_type,
                scope: row.columns.scope,
            }))
        }
    };
}
get_event_type_access_token!(
    get_external_event_token,
    ExternalEventToken,
    "external_event"
);
get_event_type_access_token!(
    get_direct_creation_token,
    DirectCreationToken,
    "direct_creation"
);

/// See `get_external_event_token`'s own doc comment's note on why this
/// one stays hand-written - the only difference from what the macro
/// generates is the extra `start_from` field, read via
/// `event_read_start_position_from_str`.
#[tracing::instrument(skip_all)]
pub async fn get_event_read_token(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<EventReadToken>> {
    let Some(row) = fetch_access_token_row(pool, id).await? else {
        return Ok(None);
    };
    if row.columns.kind != "event_read" {
        return Ok(None);
    }
    let event_type_name = row
        .columns
        .event_type_name
        .expect("event_read access_tokens row without event_type_name");
    let event_type = require_event_type(pool, &row.bounded_context, &event_type_name).await?;
    Ok(Some(EventReadToken {
        id: row.columns.id,
        secret: row.columns.secret,
        status: token_status_from_str(&row.columns.status),
        created_at: row.columns.created_at,
        revoked_at: row.columns.revoked_at,
        event_type,
        scope: row.columns.scope,
        start_from: event_read_start_position_from_str(&row.columns.start_from),
        start_at_sequence: row.columns.start_at_sequence,
        start_at_time: row.columns.start_at_time,
    }))
}

/// See `get_external_event_token`'s own doc comment - same shape and
/// `None` reasoning, resolving `command_type_name` against
/// `command_types` instead of `event_type_name` against `event_types`.
#[tracing::instrument(skip_all)]
pub async fn get_command_token(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<CommandToken>> {
    let Some(row) = fetch_access_token_row(pool, id).await? else {
        return Ok(None);
    };
    if row.columns.kind != "command" {
        return Ok(None);
    }
    let command_type_name = row
        .columns
        .command_type_name
        .expect("command access_tokens row without command_type_name");
    let command_type = require_command_type(pool, &row.bounded_context, &command_type_name).await?;
    Ok(Some(CommandToken {
        id: row.columns.id,
        secret: row.columns.secret,
        status: token_status_from_str(&row.columns.status),
        created_at: row.columns.created_at,
        revoked_at: row.columns.revoked_at,
        command_type,
        scope: row.columns.scope,
    }))
}

// --- ReadCursor ---

#[derive(sqlx::FromRow)]
struct ReadCursorRow {
    ack_mode: String,
    sequence: i64,
    updated_at: DateTime<Utc>,
    checked_out_at: Option<DateTime<Utc>>,
}

/// Unlike `access_tokens`, no index/bounded-context-resolution step is
/// needed here: every caller already holds the full `EventReadToken` (and
/// so its own `event_type.bounded_context.name`) by the time this is
/// called - token resolution (which does need the index) already
/// happened first, in `get_event_read_token` above. Generic over `impl
/// sqlx::PgExecutor` (the same generalisation `insert_event` itself
/// already has) so `GET /v1/events/consume?mode=manual` can call this
/// inside the same transaction `lock_read_cursor_for_consume`'s own
/// advisory lock spans (Codeberg issue #25's investigation,
/// docs/architecture.md §53) - see that function's own doc comment for
/// why a plain call against `&Pool` here wouldn't actually be protected
/// by a lock taken on a different pooled connection.
#[tracing::instrument(skip_all)]
pub async fn get_read_cursor(
    executor: impl sqlx::PgExecutor<'_>,
    token: &EventReadToken,
) -> crate::error::Result<Option<ReadCursor>> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    let row: Option<ReadCursorRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT ack_mode, sequence, updated_at, checked_out_at \
         FROM {schema}.read_cursors WHERE token_id = $1"
    )))
    .bind(&token.id)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|r| ReadCursor {
        token: token.clone(),
        ack_mode: ack_mode_from_str(&r.ack_mode),
        sequence: r.sequence,
        updated_at: r.updated_at,
        checked_out_at: r.checked_out_at,
    }))
}

/// The primitive that actually closes the `manual_ack` claim race
/// (Codeberg issue #25's investigation, docs/architecture.md §53) - not
/// a row lock on `read_cursors` (a `SELECT ... FOR UPDATE` protects
/// nothing for a token's very first-ever call, before any row exists to
/// lock - proven by a real concurrent test that failed exactly there
/// before this fix, not assumed). `pg_advisory_xact_lock`, the identical
/// primitive `migrate_idempotency_keys_client_id_scoping` already
/// establishes - deliberately blocking, not `pg_try_advisory_xact_lock`:
/// two callers racing the same token should serialize (one waits a few
/// milliseconds for the other's short transaction to commit), not have
/// one immediately give up. Held for the rest of the caller's own
/// transaction, released automatically on commit or rollback. Must be
/// called on the SAME transaction whose own later `get_read_cursor`/
/// `apply_cursor_update` calls it's meant to protect - a bare
/// `pg_advisory_lock` against a `&Pool` would make this meaningless (the
/// lock and the work it protects could each land on a different pooled
/// connection entirely), the identical failure mode that function's own
/// doc comment already warns about for the migration guard.
#[tracing::instrument(skip_all)]
pub async fn lock_read_cursor_for_consume(
    tx: &mut Transaction<'_, Postgres>,
    token: &EventReadToken,
) -> crate::error::Result<()> {
    let key = format!(
        "read_cursor:{}:{}",
        token.event_type.bounded_context.name, token.id
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn upsert_read_cursor(
    executor: impl sqlx::PgExecutor<'_>,
    cursor: &ReadCursor,
) -> crate::error::Result<()> {
    let schema = schema_ident(&cursor.token.event_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.read_cursors (token_id, ack_mode, sequence, updated_at, checked_out_at) \
         VALUES ($1,$2,$3,$4,$5) \
         ON CONFLICT (token_id) DO UPDATE SET \
            ack_mode = EXCLUDED.ack_mode, sequence = EXCLUDED.sequence, \
            updated_at = EXCLUDED.updated_at, checked_out_at = EXCLUDED.checked_out_at"
    )))
    .bind(&cursor.token.id)
    .bind(ack_mode_to_str(cursor.ack_mode))
    .bind(cursor.sequence)
    .bind(cursor.updated_at)
    .bind(cursor.checked_out_at)
    .execute(executor)
    .await?;
    Ok(())
}

/// Persists whatever `event_store::consume_events` decided to do to a
/// token's `ReadCursor` - see `CursorUpdate`'s own doc comment for why
/// that decision and this persistence step are two separate functions
/// (the pure/no-I/O split every rule in this crate keeps). Generic over
/// `impl sqlx::PgExecutor` (the same generalisation `insert_event` itself
/// already has) so `GET /v1/events/consume?mode=manual` can call this
/// inside the same transaction `get_read_cursor_for_update`'s own row
/// lock spans (Codeberg issue #25's investigation, docs/architecture.md
/// §53) - without that, the read and this write would each be free to
/// land on a different pooled connection, making the lock meaningless
/// (the identical failure mode `migrate_idempotency_keys_client_id_scoping`'s
/// own doc comment warns about for a bare `pg_advisory_lock`).
#[tracing::instrument(skip_all)]
pub async fn apply_cursor_update(
    executor: impl sqlx::PgExecutor<'_>,
    token: &EventReadToken,
    update: &CursorUpdate,
) -> crate::error::Result<()> {
    match update {
        CursorUpdate::Created(cursor) => upsert_read_cursor(executor, cursor).await,
        CursorUpdate::Advanced {
            sequence,
            updated_at,
        } => {
            let schema = schema_ident(&token.event_type.bounded_context.name);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.read_cursors SET sequence = $1, updated_at = $2 WHERE token_id = $3"
            )))
            .bind(sequence)
            .bind(updated_at)
            .bind(&token.id)
            .execute(executor)
            .await?;
            Ok(())
        }
        // Codeberg issue #25's investigation (docs/architecture.md §53) -
        // `sequence`/`updated_at` deliberately untouched: a claim alone
        // never moves the cursor's own position, only `AcknowledgeEvents`
        // does.
        CursorUpdate::Claimed { checked_out_at } => {
            let schema = schema_ident(&token.event_type.bounded_context.name);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.read_cursors SET checked_out_at = $1 WHERE token_id = $2"
            )))
            .bind(checked_out_at)
            .bind(&token.id)
            .execute(executor)
            .await?;
            Ok(())
        }
        CursorUpdate::Unchanged => Ok(()),
    }
}

/// Persists an `AcknowledgeEvents` outcome directly - `acknowledge_events`
/// itself only returns the new `(sequence, updated_at)` pair (it has no
/// `CursorUpdate` variant of its own; see its doc comment), so there's no
/// enum to dispatch on the way `apply_cursor_update` above does.
/// `checked_out_at` is cleared unconditionally alongside `sequence`/
/// `updated_at` - `rule AcknowledgeEvents`' own `ensures` block does the
/// same, unconditionally, regardless of whether a live claim was even
/// held (Codeberg issue #25's investigation, docs/architecture.md §53).
#[tracing::instrument(skip_all)]
pub async fn record_acknowledgement(
    pool: &Pool,
    token: &EventReadToken,
    sequence: i64,
    updated_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.read_cursors SET sequence = $1, updated_at = $2, checked_out_at = NULL \
         WHERE token_id = $3"
    )))
    .bind(sequence)
    .bind(updated_at)
    .bind(&token.id)
    .execute(pool)
    .await?;
    Ok(())
}
