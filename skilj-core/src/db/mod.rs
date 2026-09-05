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
    AccessLevel, CommandToken, DirectCreationToken, EventReadToken, ExternalEventToken,
    PrivateFieldGrant, Role, RoleAccessMapping, RoleStatus, TokenStatus,
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
use opentelemetry::metrics::{Counter, Meter};
use opentelemetry::KeyValue;
use sqlx::types::Json;
use sqlx::{Postgres, Transaction};
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
    ensure_idempotency_keys_table(&mut **tx, bounded_context).await?;
    ensure_cross_context_route_cursors_table(&mut **tx, bounded_context).await?;
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
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (projection_name, key)
        )"
    )))
    .execute(&mut **tx)
    .await?;

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
            updated_at TIMESTAMPTZ NOT NULL
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
/// `client_id`-scoped since docs/architecture.md §37 - originally
/// `(command_type_name, idempotency_key)` only (issue #12's own
/// deliberate call: "not also per-caller"), which held up fine under
/// that decision's own assumption - a well-randomized caller-chosen key
/// (a UUID, say) never collides with another caller's by accident. Owner-
/// tag multi-tenancy (§23/§25/§30), built after issue #12, broke that
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
/// variant (§36) of the same root cause. `client_id` (`token.id` for
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
/// nothing is ever deleted" precedent (§21) - but, on the user's own
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
/// duration: two skilj instances patching the same shared Postgres at
/// startup (the existing concurrent-bounded-context warm-up loop in
/// `skilj/src/lib.rs`, Codeberg issue #15, only protects against a race
/// *within* one process - a real fleet runs more than one) would
/// otherwise race the `PRIMARY KEY` swap below, which has no idempotent
/// form: a second `ADD PRIMARY KEY` after a first one already committed
/// is a hard Postgres error ("multiple primary keys ... not allowed"),
/// not a silent no-op the way `ADD COLUMN IF NOT EXISTS` is elsewhere in
/// this file. `_xact` (transaction-scoped, not session-scoped) releases
/// automatically at this function's own commit or rollback and is
/// guaranteed to run on the same connection as the statements it
/// protects, both being inside the one transaction - unlike a bare
/// `pg_advisory_lock` against a `&Pool`, where the lock and the work it
/// protects could each be handed a different pooled connection
/// entirely, making the lock meaningless.
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
            updated_at TIMESTAMPTZ NOT NULL
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
    let schema = schema_ident(bounded_context);
    let row: Option<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = $1"
    )))
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.into_domain(bc)))
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
    insert_event_and_update_sync_projections_in_tx(
        pool,
        &mut tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
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
    let bc = get_bounded_context(pool, bounded_context).await?.expect(
        "get_or_create_encryption_key: bounded_context row must exist for any caller reaching this",
    );
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
    consistency_tags: Json<Vec<Tag>>,
    consistency_boundary: Option<i64>,
}

impl CommandRow {
    async fn into_domain(
        self,
        pool: &Pool,
        bounded_context: &str,
    ) -> crate::error::Result<Command> {
        let command_type = get_command_type(pool, bounded_context, &self.command_type_name)
            .await?
            .expect("commands row references a command_types row that no longer exists");
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
    metadata_version, metadata_client_id, metadata_created_at, consistency_tags, \
    consistency_boundary";

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
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id"
    )))
    .bind(&command.id)
    .bind(&command.command_type.name)
    .bind(&command.payload)
    .bind(&command.metadata.r#type)
    .bind(command.metadata.version)
    .bind(&command.metadata.client_id)
    .bind(command.metadata.created_at)
    .bind(Json(&command.consistency_tags))
    .bind(command.consistency_boundary)
    .fetch_one(&mut **tx)
    .await?;

    for encryption_key_id in encryption_key_ids {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.command_encryption_keys (command_id, encryption_key_id) \
             VALUES ($1, $2)"
        )))
        .bind(id)
        .bind(encryption_key_id)
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
/// (§9's "keyed / multi-row Projections" pass). `ON CONFLICT ... DO
/// UPDATE SET state = {schema}.projection_state.state` is a no-op write
/// on the already-exists path - it exists purely so Postgres still
/// acquires the row lock there too (the identical guarantee a plain
/// `SELECT ... FOR UPDATE` gave the old always-pre-seeded schema),
/// without ever overwriting real accumulated state with
/// `default_state_json`. Generic over `impl sqlx::PgExecutor<'_>` (the
/// same generalisation `insert_event` itself already has) so both
/// callers can run this inside their own already-open transaction.
async fn get_or_create_projection_state_for_update(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    projection_name: &str,
    key: &str,
    default_state_json: &str,
) -> crate::error::Result<String> {
    let (state,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_state (projection_name, key, state, updated_at) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (projection_name, key) DO UPDATE SET state = {schema}.projection_state.state \
         RETURNING state"
    )))
    .bind(projection_name)
    .bind(key)
    .bind(default_state_json)
    .fetch_one(executor)
    .await?;
    Ok(state)
}

/// `get_or_create_projection_state_for_update`'s own twin for
/// `projection_rebuild_state` - see that function's own doc comment for
/// the "no-op write, purely to acquire the lock" reasoning, identical
/// here. `status` is always `'building'` inline, not a parameter - a
/// pending row is never folded (only `catch_up_bounded_context`'s own
/// `building_rebuilds` walk reaches this function at all), so there is no
/// other status any real caller could mean.
async fn get_or_create_projection_rebuild_state_for_update(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    projection_name: &str,
    key: &str,
    default_state_json: &str,
) -> crate::error::Result<String> {
    let (state,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_rebuild_state (projection_name, status, key, state, \
         updated_at) VALUES ($1, 'building', $2, $3, now()) \
         ON CONFLICT (projection_name, status, key) DO UPDATE SET \
         state = {schema}.projection_rebuild_state.state \
         RETURNING state"
    )))
    .bind(projection_name)
    .bind(key)
    .bind(default_state_json)
    .fetch_one(executor)
    .await?;
    Ok(state)
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
                "UPDATE {schema}.{table} SET state = $1, owner = $2, updated_at = now() \
                 WHERE projection_name = $3 AND key = $4{extra_where}"
            )))
            .bind(new_state)
            .bind(owner)
            .bind(projection_name)
            .bind(key)
            .execute(executor)
            .await?;
        }
        None => {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.{table} SET state = $1, updated_at = now() \
                 WHERE projection_name = $2 AND key = $3{extra_where}"
            )))
            .bind(new_state)
            .bind(projection_name)
            .bind(key)
            .execute(executor)
            .await?;
        }
    }
    Ok(())
}

/// docs/architecture.md §19's "Problem 2" - get-or-create-with-lock for
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
/// `post_commands_trigger` route (docs/architecture.md §19), so the "does
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
            let command = get_command_by_id(pool, bounded_context, command_id)
                .await?
                .expect("events row references a commands row that no longer exists");
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
    let bc = get_bounded_context(pool, bounded_context)
        .await?
        .expect("list_events: bounded_context row must exist for any event_type referencing it");
    let et = get_event_type(pool, bounded_context, event_type_name)
        .await?
        .expect("list_events: event_type row must exist for any event referencing it");

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT sequence, payload, metadata_type, metadata_version, metadata_client_id, \
         metadata_created_at, tags, origin_kind, origin_source_content, origin_source_context, \
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
    let bc = get_bounded_context(pool, bounded_context).await?.expect(
        "list_events_for_bounded_context: bounded_context row must exist for any event referencing it",
    );

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events ORDER BY sequence"
    )))
    .fetch_all(pool)
    .await?;

    let mut event_types: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        if !event_types.contains_key(&row.event_type_name) {
            let et = get_event_type(pool, bounded_context, &row.event_type_name)
                .await?
                .expect("events row references an event_types row that no longer exists");
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
    let bc = get_bounded_context(pool, bounded_context).await?.expect(
        "list_events_for_bounded_context_from: bounded_context row must exist for any event referencing it",
    );

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, tags, origin_kind, origin_source_content, \
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
            let et = get_event_type(pool, bounded_context, &row.event_type_name)
                .await?
                .expect("events row references an event_types row that no longer exists");
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
/// docs/architecture.md §19's "Problem 1" fix. Those two always fetch
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

    let bc = get_bounded_context(pool, bounded_context).await?.expect(
        "list_events_for_bounded_context_matching_tags: bounded_context row must exist for any event referencing it",
    );

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
         metadata_client_id, metadata_created_at, tags, origin_kind, origin_source_content, \
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

    let mut event_types: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        if !event_types.contains_key(&row.event_type_name) {
            let et = get_event_type(pool, bounded_context, &row.event_type_name)
                .await?
                .expect("events row references an event_types row that no longer exists");
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
            },
            sequence: row.sequence,
            tags: row.tags.0,
            encryption_keys: Vec::new(),
            origin,
        });
    }
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
         metadata_client_id, metadata_created_at, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events WHERE sequence = $1"
    )))
    .bind(sequence)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };

    let et = get_event_type(pool, bounded_context, &row.event_type_name)
        .await?
        .expect("events row references an event_types row that no longer exists");
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
    let bc = get_bounded_context(pool, bounded_context).await?.expect(
        "list_recent_events_for_bounded_context: bounded_context row must exist for any event \
         referencing it",
    );

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, tags, origin_kind, origin_source_content, \
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
            let et = get_event_type(pool, bounded_context, &row.event_type_name)
                .await?
                .expect("events row references an event_types row that no longer exists");
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
    let bc = get_bounded_context(pool, bounded_context).await?.expect(
        "list_events_from: bounded_context row must exist for any event_type referencing it",
    );
    let et = get_event_type(pool, bounded_context, event_type_name)
        .await?
        .expect("list_events_from: event_type row must exist for any event referencing it");

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT sequence, payload, metadata_type, metadata_version, metadata_client_id, \
         metadata_created_at, tags, origin_kind, origin_source_content, origin_source_context, \
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
/// docs/architecture.md §19's "Problem 1" fix, avoiding exactly the
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
         metadata_version, metadata_client_id, metadata_created_at, tags, \
         origin_kind, origin_source_content, origin_source_context, origin_command_id) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"
    )))
    .bind(event.sequence)
    .bind(&event.event_type.name)
    .bind(&event.payload)
    .bind(&event.metadata.r#type)
    .bind(event.metadata.version)
    .bind(&event.metadata.client_id)
    .bind(event.metadata.created_at)
    .bind(Json(&event.tags))
    .bind(origin_kind)
    .bind(source_content)
    .bind(source_context)
    .bind(command_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// The real, transactional half of §8 item 6: inserts `event` and, in
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
    let mut tx = pool.begin().await?;
    insert_event_and_update_sync_projections_in_tx(
        pool,
        &mut tx,
        event,
        command_id,
        dispatcher,
        encryption_key_ids,
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
/// folded in. `pool` is still needed alongside `tx`, only for
/// `list_projections_for_bounded_context`'s own metadata-only read -
/// deliberately not run through `tx` (see that call's own comment
/// below). This read is safe on the bare pool specifically *because*
/// every event-insert path already holds `next_sequence`'s own lock on
/// this bounded context's `sequence` row by the time it runs, and
/// `promote_projection_rebuild` - the one place a projection's own
/// `sync` flag can flip mid-flight - takes that identical lock before it
/// can promote (drift audit finding #6, see project memory
/// `skilj-drift-audit-2026-08-20`, and that function's own doc comment):
/// the two can never interleave, so by the time this plain read runs
/// there is no possible half-visible state to see.
#[tracing::instrument(skip_all)]
pub async fn insert_event_and_update_sync_projections_in_tx(
    pool: &Pool,
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
    command_id: Option<i64>,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    encryption_key_ids: &[i64],
) -> crate::error::Result<()> {
    let bounded_context = &event.bounded_context.name;
    let schema = schema_ident(bounded_context);

    // Metadata only - read outside `tx`, the same "small, admin-managed
    // list, not worth locking" treatment `list_projections_for_bounded_context`'s
    // own callers already give it elsewhere.
    let sync_projections: Vec<_> = list_projections_for_bounded_context(pool, bounded_context)
        .await?
        .into_iter()
        .filter(|p| p.sync)
        .collect();

    insert_event(&mut **tx, event, command_id).await?;

    // `event.encryption_keys`' own `id`s aren't carried on the domain
    // struct (it has none, matching `entity EncryptionKey` itself) -
    // `encryption_key_ids` is what `get_or_create_encryption_key` handed
    // the caller back alongside it, threaded through here so this
    // transaction can also link the join rows atomically with the event
    // insert they belong to.
    for encryption_key_id in encryption_key_ids {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.event_encryption_keys (event_sequence, encryption_key_id) \
             VALUES ($1, $2)"
        )))
        .bind(event.sequence)
        .bind(encryption_key_id)
        .execute(&mut **tx)
        .await?;
    }

    for projection in &sync_projections {
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
            let current_state = get_or_create_projection_state_for_update(
                &mut **tx,
                &schema,
                &projection.name,
                key,
                &default_state_json,
            )
            .await?;

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
    let event = crate::event_store::create_external_event(
        adapter,
        payload,
        source_content,
        source_context,
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
    insert_event_and_update_sync_projections_in_tx(
        pool,
        &mut tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
    )
    .await?;
    tx.commit().await?;
    broadcaster.publish(&event);
    record_event_appended(&event);
    notify_event_appended(pool, &event, broadcaster.instance_id()).await;
    event_cache.append(&event).await;

    Ok(event)
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
    insert_event_and_update_sync_projections_in_tx(
        pool,
        &mut tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
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
/// (docs/architecture.md §19) - `Some` when the caller took the
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
/// every real call site, docs/architecture.md §19's "Problem 1" fix)
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
    let schema = schema_ident(&bounded_context_name);
    let original_highest = bounded_context_events
        .iter()
        .map(|e| e.sequence)
        .max()
        .unwrap_or_else(|| snapshot.as_ref().map(|s| s.as_of_sequence).unwrap_or(-1));

    let mut tx = pool.begin().await?;
    let (locked_highest,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT next_value FROM {schema}.sequence FOR UPDATE"
    )))
    .fetch_one(&mut *tx)
    .await?;

    // Codeberg issue #12: a cached prior answer, not a new decision -
    // checked as early as possible, right after the lock that makes this
    // plain `SELECT` race-free (see `lookup_idempotency_key`'s own doc
    // comment). `initial_decision`/the redispatch logic below never runs
    // on a hit - `tx` is simply dropped (implicit rollback), the same as
    // every other early return in this function; nothing was written.
    if let Some(key) = idempotency_key {
        if let Some(triggered_event_sequences) =
            lookup_idempotency_key(&mut *tx, &schema, &command_type.name, client_id, key).await?
        {
            return Ok(SubmitCommandOutcome::Deduplicated {
                triggered_event_sequences,
            });
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

    if locked_highest > original_highest {
        // Something committed between the caller's own optimistic read
        // and this lock - but only a match on our own consistency_tags
        // is an actual DCB conflict; an unrelated event elsewhere in the
        // same bounded context changes nothing dispatch() would see, so
        // redispatching over it would be pure waste. docs/architecture.md
        // §19's "Problem 1" fix: `original_highest` is now the highest
        // sequence among `bounded_context_events` itself (already
        // tag-scoped by every caller of this function - see their own
        // comments), so it's exactly the DCB boundary for these tags,
        // and this delta fetch can go straight to the tag-indexed query
        // instead of an unfiltered range scan followed by an in-memory
        // tag check. This branch now runs more often than it used to in
        // a busy, multi-entity bounded context - `locked_highest`
        // reflects the whole bounded context's own latest sequence,
        // while a tag-scoped `original_highest` moves more slowly for a
        // quiet entity, so the two diverge on every commit elsewhere -
        // but each run is a small indexed query rather than a full scan,
        // so this is still a net win.
        let delta = list_events_for_bounded_context_matching_tags(
            pool,
            &bounded_context_name,
            consistency_tags,
            Some(original_highest),
        )
        .await?;
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
            return Ok(SubmitCommandOutcome::Rejected {
                reason,
                kind,
                matching_events: final_matching_events,
            });
        }
        crate::shared::CommandDecision::Accepted { events } => events,
    };

    // process_command's own resolve_event_type/next_sequence stay plain
    // sync closures (decide() and everything downstream is I/O-free per
    // §1.1) - every EventType lookup and sequence allocation this call
    // will need happens first, here, against the *final* event_specs
    // (the redispatched ones, if a retry happened above).
    let mut event_types_by_name: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    for spec in &event_specs {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            event_types_by_name.entry(spec.event_type.clone())
        {
            if let Some(et) = get_event_type(pool, &bounded_context_name, &spec.event_type).await? {
                entry.insert(et);
            }
        }
    }

    // Allocated inside `tx`, after the lock above - `next_sequence`'s
    // row lock is already held, so these UPDATEs proceed immediately,
    // and a failure anywhere below (an unregistered event type,
    // encryption resolution, the inserts themselves) rolls every one of
    // them back with the rest of this transaction.
    let mut sequences = Vec::with_capacity(event_specs.len());
    for _ in 0..event_specs.len() {
        sequences.push(next_sequence(&mut *tx, &bounded_context_name).await?);
    }
    let mut sequences = sequences.into_iter();

    // protect_sensitive_fields' own pre-resolution step, for the
    // command's own payload *and* every final event spec's - see
    // `resolve_encryption_keys`'s own doc comment. Runs against `pool`,
    // not `tx`, deliberately - EncryptionKey provisioning staying
    // outside this transaction is exactly the same "don't hold the
    // sequence row lock across a master-key wrap" reasoning
    // `create_and_insert_external_event`'s own doc comment gives, doubly
    // so here since this is the lock `submit_command` itself holds.
    let mut resolved = std::collections::HashMap::new();
    resolve_encryption_keys(
        pool,
        &bounded_context_name,
        &command_type.sensitive_fields,
        payload,
        encryption_master_key,
        &mut resolved,
    )
    .await?;
    for spec in &event_specs {
        if let Some(event_type) = event_types_by_name.get(&spec.event_type) {
            let spec_payload = spec.payload.to_string();
            resolve_encryption_keys(
                pool,
                &bounded_context_name,
                &event_type.sensitive_fields,
                &spec_payload,
                encryption_master_key,
                &mut resolved,
            )
            .await?;
        }
    }

    let result = crate::event_store::process_command(
        crate::shared::generate_token_id(),
        command_type,
        payload,
        client_id,
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
    // transaction `tx` has held since the lock above -
    // DynamicConsistencyBoundaryHonoured's actual enforcement: a failure
    // partway through this loop rolls the command insert back too,
    // rather than leaving a persisted Command with only some of its
    // events.
    let command_key_ids = encryption_key_ids(&result.command.encryption_keys, &resolved);
    let command_id = insert_command(&mut tx, &result.command, &command_key_ids).await?;
    for event in &result.events {
        let event_key_ids = encryption_key_ids(&event.encryption_keys, &resolved);
        insert_event_and_update_sync_projections_in_tx(
            pool,
            &mut tx,
            event,
            Some(command_id),
            projection_dispatcher,
            &event_key_ids,
        )
        .await?;
    }

    if let Some(key) = idempotency_key {
        let triggered_event_sequences: Vec<i64> =
            result.events.iter().map(|e| e.sequence).collect();
        insert_idempotency_key(
            &mut *tx,
            &schema,
            &command_type.name,
            client_id,
            key,
            &triggered_event_sequences,
            now,
        )
        .await?;
    }

    tx.commit().await?;

    // EventSubscription's own real-time delivery - after the commit, not
    // before, the same rule `insert_event_and_update_sync_projections`
    // itself already follows.
    for event in &result.events {
        broadcaster.publish(event);
        record_event_appended(event);
        notify_event_appended(pool, event, broadcaster.instance_id()).await;
        event_cache.append(event).await;
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

/// The full "optimistic decide, then locked submit" sequence
/// `ProcessCommand` describes end to end, for a caller that already has
/// a resolved `CommandType` and a JSON payload in hand: derive
/// consistency tags, resolve a snapshot context if one applies
/// (`resolve_snapshot_context`), fetch matching events (tag-indexed -
/// docs/architecture.md §19's own "Problem 1" fix), `dispatch()` once
/// optimistically, then hand off to [`submit_command`] for the real,
/// locked recheck-and-retry.
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
    encryption_master_key: Option<&EncryptionMasterKey>,
    now: DateTime<Utc>,
    idempotency_key: Option<&str>,
) -> crate::error::Result<SubmitCommandOutcome> {
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

    submit_command(
        pool,
        dispatcher,
        projection_dispatcher,
        broadcaster,
        event_cache,
        command_type,
        payload,
        client_id,
        &bounded_context_events,
        &consistency_tags,
        &matching_events,
        decision,
        encryption_master_key,
        now,
        snapshot_context.as_ref().map(|ctx| SnapshotContext {
            state_json: &ctx.state_json,
            as_of_sequence: ctx.as_of_sequence,
        }),
        idempotency_key,
    )
    .await
}

/// `cross_context_route_cursors`'s own read - `-1` (the same "nothing
/// yet" sentinel `sequence`/`caught_up_to` already use) when this route
/// has never dispatched anything yet, whether because no row exists at
/// all or because `updated_at` predates the table's own creation for
/// this bounded context (patched in retroactively - see
/// `ensure_cross_context_route_cursors_table`'s own doc comment).
async fn get_cross_context_route_cursor(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
) -> crate::error::Result<i64> {
    let schema = schema_ident(source_bounded_context);
    let row: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT last_dispatched_sequence FROM {schema}.cross_context_route_cursors \
         WHERE route_name = $1"
    )))
    .bind(route_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(seq,)| seq).unwrap_or(-1))
}

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
         updated_at = EXCLUDED.updated_at"
    )))
    .bind(route_name)
    .bind(sequence)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
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
) -> crate::error::Result<()> {
    let cursor =
        get_cross_context_route_cursor(pool, route.source_bounded_context, route.name).await?;
    let events = list_events_cached(
        pool,
        event_cache,
        route.source_bounded_context,
        route.source_event_type,
        cursor,
    )
    .await?;

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
                    encryption_master_key,
                    Utc::now(),
                    Some(&idempotency_key),
                )
                .await?;
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
    }
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

/// The background half of §8 item 6: one poll tick, for one bounded
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
    let async_projections: Vec<_> = all_projections.iter().filter(|p| !p.sync).collect();
    let building_rebuilds = list_building_projection_rebuilds_for_bounded_context(
        pool,
        bounded_context,
        &all_projections,
    )
    .await?;

    if async_projections.is_empty() && building_rebuilds.is_empty() {
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

    let min_caught_up = async_projections
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

        for projection in &async_projections {
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
                let current_state = get_or_create_projection_state_for_update(
                    &mut *tx,
                    &schema,
                    &projection.name,
                    key,
                    &default_state_json,
                )
                .await?;

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
                let current_state = get_or_create_projection_rebuild_state_for_update(
                    &mut *tx,
                    &schema,
                    &rebuild.projection.name,
                    key,
                    &default_state_json,
                )
                .await?;

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

    Ok(())
}

/// docs/architecture.md §19's "Problem 2" - the background half of
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

    let snapshot_names = dispatcher.snapshot_names(bounded_context);
    if snapshot_names.is_empty() {
        return Ok(());
    }

    let progress_from_db =
        list_snapshot_progress_for_bounded_context(pool, bounded_context).await?;
    let mut progress: std::collections::HashMap<&str, i64> = snapshot_names
        .iter()
        .map(|name| (*name, progress_from_db.get(*name).copied().unwrap_or(-1)))
        .collect();

    let min_caught_up = progress.values().copied().min().unwrap_or(latest);
    let events = if min_caught_up >= latest {
        Vec::new()
    } else {
        list_events_for_bounded_context_from(pool, bounded_context, min_caught_up).await?
    };

    for event in &events {
        let mut tx = pool.begin().await?;

        for name in &snapshot_names {
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

        for name in &snapshot_names {
            if progress[name] >= event.sequence {
                continue;
            }
            upsert_snapshot_progress(&mut *tx, &schema, name, event.sequence).await?;
            progress.insert(*name, event.sequence);
        }

        tx.commit().await?;
    }

    Ok(())
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
            let current_state = get_or_create_projection_state_for_update(
                &mut *tx,
                &schema,
                &projection.name,
                key,
                &default_state_json,
            )
            .await?;

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
    // write-up of this pass).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_state (projection_name, key, state, owner, updated_at) \
         SELECT projection_name, key, state, owner, updated_at \
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
         command_type_name, scope FROM {schema}.access_tokens WHERE id = $1"
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

#[tracing::instrument(skip_all)]
pub async fn insert_event_read_token(
    pool: &Pool,
    token: &EventReadToken,
) -> crate::error::Result<()> {
    insert_access_token_row(
        pool,
        &token.id,
        AccessTokenKind::EventRead,
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
/// `get_direct_creation_token`/`get_event_read_token`, identical apart
/// from the `kind` string and return type - all three resolve
/// `event_type_name` against `event_types` into an `event_type` field
/// and now carry `scope` alike (cross-tenant read/write fix,
/// docs/architecture.md's own write-up of these passes - `EventReadToken`
/// was the only one of the four token kinds this was true for before
/// that fix's write-side half, which is why this used to be two macro
/// invocations plus a near-identical hand-written twin; now all three
/// genuinely share one shape). `get_command_token` resolves
/// `command_type_name` into a differently named field instead and is the
/// only one of its own kind, so it stays hand-written below.
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
            let event_type = get_event_type(pool, &row.bounded_context, &event_type_name)
                .await?
                .expect("access_tokens row references an event_type that no longer exists");
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
get_event_type_access_token!(get_event_read_token, EventReadToken, "event_read");

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
    let command_type = get_command_type(pool, &row.bounded_context, &command_type_name)
        .await?
        .expect("access_tokens row references a command_type that no longer exists");
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
}

/// Unlike `access_tokens`, no index/bounded-context-resolution step is
/// needed here: every caller already holds the full `EventReadToken` (and
/// so its own `event_type.bounded_context.name`) by the time this is
/// called - token resolution (which does need the index) already
/// happened first, in `get_event_read_token` above.
#[tracing::instrument(skip_all)]
pub async fn get_read_cursor(
    pool: &Pool,
    token: &EventReadToken,
) -> crate::error::Result<Option<ReadCursor>> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    let row: Option<ReadCursorRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT ack_mode, sequence, updated_at FROM {schema}.read_cursors WHERE token_id = $1"
    )))
    .bind(&token.id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| ReadCursor {
        token: token.clone(),
        ack_mode: ack_mode_from_str(&r.ack_mode),
        sequence: r.sequence,
        updated_at: r.updated_at,
    }))
}

async fn upsert_read_cursor(pool: &Pool, cursor: &ReadCursor) -> crate::error::Result<()> {
    let schema = schema_ident(&cursor.token.event_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.read_cursors (token_id, ack_mode, sequence, updated_at) \
         VALUES ($1,$2,$3,$4) \
         ON CONFLICT (token_id) DO UPDATE SET \
            ack_mode = EXCLUDED.ack_mode, sequence = EXCLUDED.sequence, \
            updated_at = EXCLUDED.updated_at"
    )))
    .bind(&cursor.token.id)
    .bind(ack_mode_to_str(cursor.ack_mode))
    .bind(cursor.sequence)
    .bind(cursor.updated_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Persists whatever `event_store::consume_events` decided to do to a
/// token's `ReadCursor` - see `CursorUpdate`'s own doc comment for why
/// that decision and this persistence step are two separate functions
/// (the pure/no-I/O split every rule in this crate keeps).
#[tracing::instrument(skip_all)]
pub async fn apply_cursor_update(
    pool: &Pool,
    token: &EventReadToken,
    update: &CursorUpdate,
) -> crate::error::Result<()> {
    match update {
        CursorUpdate::Created(cursor) => upsert_read_cursor(pool, cursor).await,
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
            .execute(pool)
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
#[tracing::instrument(skip_all)]
pub async fn record_acknowledgement(
    pool: &Pool,
    token: &EventReadToken,
    sequence: i64,
    updated_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.read_cursors SET sequence = $1, updated_at = $2 WHERE token_id = $3"
    )))
    .bind(sequence)
    .bind(updated_at)
    .bind(&token.id)
    .execute(pool)
    .await?;
    Ok(())
}
