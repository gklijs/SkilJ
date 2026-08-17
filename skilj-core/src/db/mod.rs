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
    AccessLevel, CommandToken, DirectCreationToken, EventReadToken, ExternalEventToken, Role,
    RoleAccessMapping, RoleStatus, TokenStatus,
};
use crate::bootstrap::ContextCreator;
use crate::event_store::{
    AckMode, BoundedContext, BoundedContextStatus, Command, CommandType, CursorUpdate, Event,
    EventOrigin, EventType, ReadCursor,
};
use crate::projections::{Projection, ProjectionRebuild, ProjectionRebuildStatus};
use crate::shared::{Metadata, SensitiveField, Tag, TagMapping};
use chrono::{DateTime, Utc};
use sqlx::types::Json;
use sqlx::{Postgres, Transaction};

/// An opaque handle to the connection pool - re-exported so `skilj-rest`/
/// `skilj-graphql` can hold one without depending on `sqlx` directly
/// themselves, the same crate-boundary reasoning docs/architecture.md
/// §3.1 gives for keeping `skilj-core` the only crate that owns the
/// database driver.
pub type Pool = sqlx::PgPool;

pub async fn connect(database_url: &str) -> Result<Pool, sqlx::Error> {
    sqlx::PgPool::connect(database_url).await
}

/// Runs every embedded migration under `skilj-core/migrations/` - see
/// this module's own doc comment and docs/architecture.md §2.2 for why
/// `skilj-core` owns its schema this way. Only ever touches the global
/// tables - per-bounded-context schemas are provisioned separately, see
/// `provision_bounded_context_schema`.
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

    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&mut **tx)
        .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.event_types (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            tag_mappings JSONB NOT NULL DEFAULT '[]',
            sensitive_fields JSONB NOT NULL DEFAULT '[]',
            external_creation_allowed BOOLEAN NOT NULL,
            direct_creation_allowed BOOLEAN NOT NULL,
            system_triggered_allowed BOOLEAN NOT NULL,
            system_triggered_schedule TEXT,
            event_read_allowed BOOLEAN NOT NULL
        )"
    ))
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.command_types (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            tag_mappings JSONB NOT NULL DEFAULT '[]',
            sensitive_fields JSONB NOT NULL DEFAULT '[]',
            rest_trigger_allowed BOOLEAN NOT NULL
        )"
    ))
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.commands (
            id BIGSERIAL PRIMARY KEY,
            command_type_name TEXT NOT NULL REFERENCES {schema}.command_types (name),
            payload TEXT NOT NULL,
            metadata_type TEXT NOT NULL,
            metadata_version BIGINT NOT NULL,
            metadata_client_id TEXT NOT NULL,
            metadata_created_at TIMESTAMPTZ NOT NULL,
            encryption_keys JSONB NOT NULL DEFAULT '[]',
            consistency_tags JSONB NOT NULL DEFAULT '[]',
            consistency_boundary BIGINT
        )"
    ))
    .execute(&mut **tx)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX commands_by_created_at ON {schema}.commands (metadata_created_at)"
    ))
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.projections (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            sync BOOLEAN NOT NULL,
            caught_up_to BIGINT
        )"
    ))
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.projection_consumed_event_types (
            projection_name TEXT NOT NULL REFERENCES {schema}.projections (name),
            event_type_name TEXT NOT NULL REFERENCES {schema}.event_types (name),
            PRIMARY KEY (projection_name, event_type_name)
        )"
    ))
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.projection_rebuilds (
            projection_name TEXT PRIMARY KEY REFERENCES {schema}.projections (name),
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            sync BOOLEAN NOT NULL,
            caught_up_to BIGINT,
            status TEXT NOT NULL CHECK (status IN ('pending', 'building'))
        )"
    ))
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.projection_rebuild_consumed_event_types (
            projection_name TEXT NOT NULL REFERENCES {schema}.projection_rebuilds (projection_name),
            event_type_name TEXT NOT NULL REFERENCES {schema}.event_types (name),
            PRIMARY KEY (projection_name, event_type_name)
        )"
    ))
    .execute(&mut **tx)
    .await?;

    // A building `ProjectionRebuild`'s own private fold - "second ...
    // nothing reads until it is complete" (the note above
    // `RegisterProjection`), never the same row as the live projection's
    // own `projection_state` below, since both can exist at once during a
    // replay window. Seeded lazily by `catch_up_bounded_context`, not
    // here or at registration time - unlike `projection_state`, there is
    // no single call site that always has the right starting value (see
    // `ProjectionDispatcher::default_state`'s own doc comment).
    sqlx::query(&format!(
        "CREATE TABLE {schema}.projection_rebuild_state (
            projection_name TEXT PRIMARY KEY REFERENCES {schema}.projection_rebuilds (projection_name),
            state TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )"
    ))
    .execute(&mut **tx)
    .await?;

    // A sync projection's own materialised state (`project()`'s own
    // fold output, JSON-encoded) - see docs/architecture.md's write-up
    // of §8 item 6. Seeded with `T::State::default()` at registration
    // time (`upsert_projection`), not created lazily here or on first
    // fold, so `insert_event_and_update_sync_projections`'s own row lock
    // always finds a row to lock rather than needing an insert-or-update
    // branch.
    sqlx::query(&format!(
        "CREATE TABLE {schema}.projection_state (
            projection_name TEXT PRIMARY KEY REFERENCES {schema}.projections (name),
            state TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )"
    ))
    .execute(&mut **tx)
    .await?;

    // Backs `next_sequence` - one row, seeded below, incremented under
    // the row lock its own `UPDATE ... RETURNING` acquires. `-1` is
    // "nothing allocated yet", so the first call returns `0` - the same
    // convention `after_sequence.unwrap_or(-1)`/`highest_sequence`'s
    // `None` case use throughout skilj-core.
    sqlx::query(&format!(
        "CREATE TABLE {schema}.sequence (next_value BIGINT NOT NULL)"
    ))
    .execute(&mut **tx)
    .await?;
    sqlx::query(&format!(
        "INSERT INTO {schema}.sequence (next_value) VALUES (-1)"
    ))
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.events (
            sequence BIGINT PRIMARY KEY,
            event_type_name TEXT NOT NULL REFERENCES {schema}.event_types (name),
            payload TEXT NOT NULL,
            metadata_type TEXT NOT NULL,
            metadata_version BIGINT NOT NULL,
            metadata_client_id TEXT NOT NULL,
            metadata_created_at TIMESTAMPTZ NOT NULL,
            tags JSONB NOT NULL DEFAULT '[]',
            encryption_keys JSONB NOT NULL DEFAULT '[]',
            origin_kind TEXT NOT NULL CHECK (
                origin_kind IN ('external_triggered', 'directly_created', 'command_triggered', 'system_triggered')
            ),
            origin_source_content TEXT,
            origin_source_context TEXT,
            origin_command_id BIGINT REFERENCES {schema}.commands (id)
        )"
    ))
    .execute(&mut **tx)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX events_by_type ON {schema}.events (event_type_name, sequence)"
    ))
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.access_tokens (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK (kind IN ('external_event', 'direct_creation', 'event_read', 'command')),
            secret TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
            created_at TIMESTAMPTZ NOT NULL,
            revoked_at TIMESTAMPTZ,
            event_type_name TEXT REFERENCES {schema}.event_types (name),
            command_type_name TEXT REFERENCES {schema}.command_types (name),
            CHECK (
                (kind = 'command' AND command_type_name IS NOT NULL AND event_type_name IS NULL)
                OR (kind != 'command' AND event_type_name IS NOT NULL AND command_type_name IS NULL)
            )
        )"
    ))
    .execute(&mut **tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE {schema}.read_cursors (
            token_id TEXT PRIMARY KEY REFERENCES {schema}.access_tokens (id),
            ack_mode TEXT NOT NULL CHECK (ack_mode IN ('auto_advance', 'manual_ack')),
            sequence BIGINT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )"
    ))
    .execute(&mut **tx)
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
pub async fn hard_delete_bounded_context(pool: &Pool, name: &str) -> crate::error::Result<()> {
    let schema = schema_ident(name);
    let mut tx = pool.begin().await?;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM bounded_contexts WHERE name = $1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
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

pub async fn insert_role(pool: &Pool, role: &Role) -> crate::error::Result<()> {
    sqlx::query(&format!(
        "INSERT INTO roles ({ROLE_COLUMNS}) VALUES ($1,$2,$3,$4,$5,$6,$7)"
    ))
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

pub async fn get_role(pool: &Pool, id: &str) -> crate::error::Result<Option<Role>> {
    let row: Option<RoleRow> =
        sqlx::query_as(&format!("SELECT {ROLE_COLUMNS} FROM roles WHERE id = $1"))
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
pub async fn list_roles(pool: &Pool) -> crate::error::Result<Vec<Role>> {
    let rows: Vec<RoleRow> = sqlx::query_as(&format!("SELECT {ROLE_COLUMNS} FROM roles"))
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(RoleRow::into_domain).collect())
}

/// Persists whatever `revoke_role`/`create_superadmin` (or any future
/// rule) produced - a full-row overwrite by `id`, not an upsert; the row
/// must already exist (`insert_role` already ran).
pub async fn update_role(pool: &Pool, role: &Role) -> crate::error::Result<()> {
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
    .execute(pool)
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
}

const BOUNDED_CONTEXT_COLUMNS: &str =
    "name, status, created_at, created_by_kind, created_by_role_id";

/// Provisions the new context's own `bc_<name>` schema (see
/// `provision_bounded_context_schema`) and inserts its `bounded_contexts`
/// registry row in one transaction - a failure partway through either
/// half rolls back the other, so there's never an orphaned schema with
/// no registry row or vice versa. Callers (test fixtures included) don't
/// need to know any of this happens; the signature is unchanged from
/// before schema-per-context existed.
pub async fn insert_bounded_context(pool: &Pool, bc: &BoundedContext) -> crate::error::Result<()> {
    let (kind, role_id) = match &bc.created_by {
        ContextCreator::SystemCreator => ("system", None),
        ContextCreator::SuperadminCreator { role } => ("superadmin", Some(role.id.clone())),
    };

    let mut tx = pool.begin().await?;
    provision_bounded_context_schema(&mut tx, &bc.name).await?;
    sqlx::query(&format!(
        "INSERT INTO bounded_contexts ({BOUNDED_CONTEXT_COLUMNS}) VALUES ($1,$2,$3,$4,$5)"
    ))
    .bind(&bc.name)
    .bind(bounded_context_status_to_str(bc.status))
    .bind(bc.created_at)
    .bind(kind)
    .bind(role_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Persists an `archive_bounded_context` outcome - a status-only update,
/// the only field any rule in this crate ever changes on an existing
/// `bounded_contexts` row (`name`/`created_at`/`created_by` are fixed at
/// creation - `rule AddBoundedContext`'s own "chosen once, at the moment
/// of creation" framing, and there is no rename). Distinct from
/// `insert_bounded_context`, which provisions a brand new schema - this
/// touches only the registry row of one that already exists.
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
    Ok(())
}

/// Unlike most `into_domain`-style conversions in this module,
/// `ContextCreator::SuperadminCreator` needs a second query (the
/// referenced `roles` row - see the migration's own note on why
/// `bounded_contexts` normalises onto `roles` rather than denormalising
/// its fields directly), so this is a free function taking the row plus
/// an already-resolved `Option<Role>`, not a method on the row type.
fn bounded_context_from_row(row: BoundedContextRow, role: Option<Role>) -> BoundedContext {
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
    }
}

pub async fn get_bounded_context(
    pool: &Pool,
    name: &str,
) -> crate::error::Result<Option<BoundedContext>> {
    let Some(row): Option<BoundedContextRow> = sqlx::query_as(&format!(
        "SELECT {BOUNDED_CONTEXT_COLUMNS} FROM bounded_contexts WHERE name = $1"
    ))
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
    Ok(Some(bounded_context_from_row(row, role)))
}

/// Every `BoundedContext` this engine currently knows of - the
/// `bounded_contexts` parameter `bootstrap::list_bounded_contexts`
/// expects (see its own doc comment: unrestricted, same full-snapshot
/// treatment `list_roles`/`list_role_access_mappings` get). Backs the
/// `BoundedContextDirectory` surface's `boundedContexts` query.
pub async fn list_bounded_contexts(pool: &Pool) -> crate::error::Result<Vec<BoundedContext>> {
    let rows: Vec<BoundedContextRow> = sqlx::query_as(&format!(
        "SELECT {BOUNDED_CONTEXT_COLUMNS} FROM bounded_contexts"
    ))
    .fetch_all(pool)
    .await?;

    let mut contexts = Vec::with_capacity(rows.len());
    for row in rows {
        let role = match &row.created_by_role_id {
            Some(role_id) => Some(get_role(pool, role_id).await?.expect(
                "bounded_contexts.created_by_role_id references a roles row that no longer exists",
            )),
            None => None,
        };
        contexts.push(bounded_context_from_row(row, role));
    }
    Ok(contexts)
}

// --- EventType ---

#[derive(sqlx::FromRow)]
struct EventTypeRow {
    name: String,
    schema: String,
    schema_version: i64,
    tag_mappings: Json<Vec<TagMapping>>,
    sensitive_fields: Json<Vec<SensitiveField>>,
    external_creation_allowed: bool,
    direct_creation_allowed: bool,
    system_triggered_allowed: bool,
    system_triggered_schedule: Option<String>,
    event_read_allowed: bool,
}

impl EventTypeRow {
    fn into_domain(self, bounded_context: BoundedContext) -> EventType {
        EventType {
            bounded_context,
            name: self.name,
            schema: self.schema,
            schema_version: self.schema_version,
            tag_mappings: self.tag_mappings.0,
            sensitive_fields: self.sensitive_fields.0,
            external_creation_allowed: self.external_creation_allowed,
            direct_creation_allowed: self.direct_creation_allowed,
            system_triggered_allowed: self.system_triggered_allowed,
            system_triggered_schedule: self.system_triggered_schedule,
            event_read_allowed: self.event_read_allowed,
        }
    }
}

const EVENT_TYPE_COLUMNS: &str = "name, schema, schema_version, tag_mappings, sensitive_fields, \
    external_creation_allowed, direct_creation_allowed, system_triggered_allowed, \
    system_triggered_schedule, event_read_allowed";

/// Upsert, not insert-only - `RegisterEventType`'s own create-or-update
/// shape (see `event_store::register_event_type`), though no surface
/// calls this yet this pass; used directly by tests/seeding until
/// `RegisterEventType` itself has a GraphQL route in front of it.
pub async fn upsert_event_type(pool: &Pool, et: &EventType) -> crate::error::Result<()> {
    let schema = schema_ident(&et.bounded_context.name);
    sqlx::query(&format!(
        "INSERT INTO {schema}.event_types ({EVENT_TYPE_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
         ON CONFLICT (name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            tag_mappings = EXCLUDED.tag_mappings, sensitive_fields = EXCLUDED.sensitive_fields, \
            external_creation_allowed = EXCLUDED.external_creation_allowed, \
            direct_creation_allowed = EXCLUDED.direct_creation_allowed, \
            system_triggered_allowed = EXCLUDED.system_triggered_allowed, \
            system_triggered_schedule = EXCLUDED.system_triggered_schedule, \
            event_read_allowed = EXCLUDED.event_read_allowed"
    ))
    .bind(&et.name)
    .bind(&et.schema)
    .bind(et.schema_version)
    .bind(Json(&et.tag_mappings))
    .bind(Json(&et.sensitive_fields))
    .bind(et.external_creation_allowed)
    .bind(et.direct_creation_allowed)
    .bind(et.system_triggered_allowed)
    .bind(&et.system_triggered_schedule)
    .bind(et.event_read_allowed)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_event_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<EventType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let row: Option<EventTypeRow> = sqlx::query_as(&format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = $1"
    ))
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.into_domain(bc)))
}

// --- CommandType ---

#[derive(sqlx::FromRow)]
struct CommandTypeRow {
    name: String,
    schema: String,
    schema_version: i64,
    tag_mappings: Json<Vec<TagMapping>>,
    sensitive_fields: Json<Vec<SensitiveField>>,
    rest_trigger_allowed: bool,
}

impl CommandTypeRow {
    fn into_domain(self, bounded_context: BoundedContext) -> CommandType {
        CommandType {
            bounded_context,
            name: self.name,
            schema: self.schema,
            schema_version: self.schema_version,
            tag_mappings: self.tag_mappings.0,
            sensitive_fields: self.sensitive_fields.0,
            rest_trigger_allowed: self.rest_trigger_allowed,
        }
    }
}

const COMMAND_TYPE_COLUMNS: &str =
    "name, schema, schema_version, tag_mappings, sensitive_fields, rest_trigger_allowed";

/// See `upsert_event_type` above - same shape and reasoning.
pub async fn upsert_command_type(pool: &Pool, ct: &CommandType) -> crate::error::Result<()> {
    let schema = schema_ident(&ct.bounded_context.name);
    sqlx::query(&format!(
        "INSERT INTO {schema}.command_types ({COMMAND_TYPE_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6) \
         ON CONFLICT (name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            tag_mappings = EXCLUDED.tag_mappings, sensitive_fields = EXCLUDED.sensitive_fields, \
            rest_trigger_allowed = EXCLUDED.rest_trigger_allowed"
    ))
    .bind(&ct.name)
    .bind(&ct.schema)
    .bind(ct.schema_version)
    .bind(Json(&ct.tag_mappings))
    .bind(Json(&ct.sensitive_fields))
    .bind(ct.rest_trigger_allowed)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_command_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<CommandType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let row: Option<CommandTypeRow> = sqlx::query_as(&format!(
        "SELECT {COMMAND_TYPE_COLUMNS} FROM {schema}.command_types WHERE name = $1"
    ))
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.into_domain(bc)))
}

// --- Command ---

#[derive(sqlx::FromRow)]
struct CommandRow {
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

const COMMAND_COLUMNS: &str = "command_type_name, payload, metadata_type, metadata_version, \
    metadata_client_id, metadata_created_at, consistency_tags, consistency_boundary";

/// Insert-only, unlike every `upsert_*` above - a `Command` has no
/// identity to update against (see the migration's own doc comment on
/// `commands`), so every call is a new row. Returns the new row's `id`,
/// needed to link the `Event`s `process_command` produced back to it (see
/// `insert_event`'s own `command_id` parameter).
pub async fn insert_command(pool: &Pool, command: &Command) -> crate::error::Result<i64> {
    debug_assert!(
        command.encryption_keys.is_empty(),
        "insert_command: EncryptionKey persistence isn't wired yet - protect_sensitive_fields' \
         non-empty branch is still todo!(), so no caller of this function can produce one"
    );
    let schema = schema_ident(&command.bounded_context.name);
    let (id,): (i64,) = sqlx::query_as(&format!(
        "INSERT INTO {schema}.commands ({COMMAND_COLUMNS}, encryption_keys) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id"
    ))
    .bind(&command.command_type.name)
    .bind(&command.payload)
    .bind(&command.metadata.r#type)
    .bind(command.metadata.version)
    .bind(&command.metadata.client_id)
    .bind(command.metadata.created_at)
    .bind(Json(&command.consistency_tags))
    .bind(command.consistency_boundary)
    .bind(Json(Vec::<serde_json::Value>::new()))
    .fetch_one(pool)
    .await?;
    Ok(id)
}

pub async fn get_command_by_id(
    pool: &Pool,
    bounded_context: &str,
    id: i64,
) -> crate::error::Result<Option<Command>> {
    let schema = schema_ident(bounded_context);
    let row: Option<CommandRow> = sqlx::query_as(&format!(
        "SELECT {COMMAND_COLUMNS} FROM {schema}.commands WHERE id = $1"
    ))
    .bind(id)
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
pub async fn list_commands_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Command>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<CommandRow> =
        sqlx::query_as(&format!("SELECT {COMMAND_COLUMNS} FROM {schema}.commands"))
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
    let names: Vec<(String,)> = sqlx::query_as(&format!(
        "SELECT event_type_name FROM {schema}.{join_table} WHERE projection_name = $1"
    ))
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
    pool: &Pool,
    bounded_context: &str,
    join_table: &str,
    projection_name: &str,
    event_types: &[EventType],
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(&format!(
        "DELETE FROM {schema}.{join_table} WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .execute(pool)
    .await?;
    for et in event_types {
        sqlx::query(&format!(
            "INSERT INTO {schema}.{join_table} (projection_name, event_type_name) VALUES ($1,$2)"
        ))
        .bind(projection_name)
        .bind(&et.name)
        .execute(pool)
        .await?;
    }
    Ok(())
}

const PROJECTION_COLUMNS: &str = "name, schema, schema_version, sync, caught_up_to";

/// See `upsert_event_type` above - same upsert shape, plus replacing this
/// projection's `projection_consumed_event_types` join rows wholesale
/// (simpler and plenty fast enough for a small, admin-managed list than
/// diffing old vs. new membership).
pub async fn upsert_projection(pool: &Pool, projection: &Projection) -> crate::error::Result<()> {
    let schema = schema_ident(&projection.bounded_context.name);
    sqlx::query(&format!(
        "INSERT INTO {schema}.projections ({PROJECTION_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5) \
         ON CONFLICT (name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            sync = EXCLUDED.sync, caught_up_to = EXCLUDED.caught_up_to"
    ))
    .bind(&projection.name)
    .bind(&projection.schema)
    .bind(projection.schema_version)
    .bind(projection.sync)
    .bind(projection.caught_up_to)
    .execute(pool)
    .await?;
    replace_consumed_event_types(
        pool,
        &projection.bounded_context.name,
        "projection_consumed_event_types",
        &projection.name,
        &projection.consumed_event_types,
    )
    .await
}

/// Seeds a projection's own `projection_state` row with
/// `default_state_json` (`T::State::default()`, serialised) - a no-op
/// when a row already exists (`ON CONFLICT ... DO NOTHING`), so this is
/// safe to call on every reconciliation pass, not only the first one
/// that ever creates the projection - see `provision_bounded_context_schema`'s
/// own note on why `projection_state` is seeded here rather than
/// created lazily on first fold.
pub async fn seed_projection_state(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    default_state_json: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(&format!(
        "INSERT INTO {schema}.projection_state (projection_name, state, updated_at) \
         VALUES ($1, $2, now()) \
         ON CONFLICT (projection_name) DO NOTHING"
    ))
    .bind(projection_name)
    .bind(default_state_json)
    .execute(pool)
    .await?;
    Ok(())
}

/// A projection's own materialised state, JSON-encoded - `None` only
/// when the projection itself isn't registered at all (a real row always
/// exists otherwise, seeded by `seed_projection_state` at registration
/// time). For whoever eventually builds `ProjectionQuery`'s own
/// `read_projection` - not read by anything in this pass, since that
/// surface isn't built yet (§8 item 6's own scope note).
pub async fn get_projection_state(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
) -> crate::error::Result<Option<String>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT state FROM {schema}.projection_state WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(state,)| state))
}

/// A building `ProjectionRebuild`'s own materialised state, JSON-encoded.
/// `None` when either nothing is staged at all or a staged rebuild hasn't
/// been seeded yet (`catch_up_bounded_context` hasn't reached it in a
/// poll tick yet, or the dispatcher has never recognised it - see
/// `ProjectionDispatcher::default_state`). Mirrors `get_projection_state`
/// above - for tests and anything else wanting to read this row directly
/// outside the consumer's own row-locked transaction.
pub async fn get_projection_rebuild_state(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
) -> crate::error::Result<Option<String>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT state FROM {schema}.projection_rebuild_state WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(state,)| state))
}

pub async fn get_projection(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<Projection>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let Some(row): Option<ProjectionRow> = sqlx::query_as(&format!(
        "SELECT {PROJECTION_COLUMNS} FROM {schema}.projections WHERE name = $1"
    ))
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
pub async fn list_projections_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Projection>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<ProjectionRow> = sqlx::query_as(&format!(
        "SELECT {PROJECTION_COLUMNS} FROM {schema}.projections"
    ))
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

/// Upsert, matching `RegisterProjection`'s own "restaging replaces the
/// pending row" semantics (see the migration's own doc comment on why
/// `projection_rebuilds` is keyed 1:1 by projection rather than a
/// synthetic id). Note that `register_projection`'s own struct-update
/// construction (`ProjectionRebuild { caught_up_to: None, ..staged.clone() }`)
/// sets `caught_up_to` back to `None` on *every* call this makes,
/// including a restage of a row already `building` - `rebuild.status`
/// alone doesn't change, but any progress `catch_up_bounded_context` had
/// already made toward the old `schema`/`consumed_event_types` is
/// invalidated the moment this is called. This function doesn't touch
/// `projection_rebuild_state` itself - `catch_up_bounded_context` is what
/// notices `caught_up_to = None` and resets that row, using whatever the
/// *current* dispatcher considers the default (see
/// `ProjectionDispatcher::default_state`'s own doc comment for why it,
/// not this function, has to be the one deciding that value).
pub async fn upsert_projection_rebuild(
    pool: &Pool,
    rebuild: &ProjectionRebuild,
) -> crate::error::Result<()> {
    let schema = schema_ident(&rebuild.projection.bounded_context.name);
    sqlx::query(&format!(
        "INSERT INTO {schema}.projection_rebuilds ({PROJECTION_REBUILD_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6) \
         ON CONFLICT (projection_name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            sync = EXCLUDED.sync, caught_up_to = EXCLUDED.caught_up_to, status = EXCLUDED.status"
    ))
    .bind(&rebuild.projection.name)
    .bind(&rebuild.schema)
    .bind(rebuild.schema_version)
    .bind(rebuild.sync)
    .bind(rebuild.caught_up_to)
    .bind(projection_rebuild_status_to_str(rebuild.status))
    .execute(pool)
    .await?;
    replace_consumed_event_types(
        pool,
        &rebuild.projection.bounded_context.name,
        "projection_rebuild_consumed_event_types",
        &rebuild.projection.name,
        &rebuild.consumed_event_types,
    )
    .await
}

/// `None` when this projection has no pending/building rebuild staged -
/// the `staged` parameter `register_projection`/`rebuild_projection`/
/// `discard_projection_rebuild` each expect "as already looked up by the
/// caller" (see their own doc comments).
pub async fn get_projection_rebuild(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
) -> crate::error::Result<Option<ProjectionRebuild>> {
    let Some(projection) = get_projection(pool, bounded_context, projection_name).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let Some(row): Option<ProjectionRebuildRow> = sqlx::query_as(&format!(
        "SELECT {PROJECTION_REBUILD_COLUMNS} FROM {schema}.projection_rebuilds \
         WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    debug_assert_eq!(
        row.projection_name, projection_name,
        "projection_rebuilds row matched the WHERE clause but its own projection_name column disagrees"
    );
    let consumed = consumed_event_types(
        pool,
        bounded_context,
        "projection_rebuild_consumed_event_types",
        projection_name,
    )
    .await?;
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
/// caller-side deletion it hands back to.
pub async fn delete_projection_rebuild(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(&format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        "DELETE FROM {schema}.projection_rebuilds WHERE projection_name = $1"
    ))
    .bind(projection_name)
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
    async fn into_domain(self, pool: &Pool) -> crate::error::Result<RoleAccessMapping> {
        let role = get_role(pool, &self.role_id)
            .await?
            .expect("role_access_mappings row references a roles row that no longer exists");
        let bounded_context = get_bounded_context(pool, &self.bounded_context)
            .await?
            .expect(
                "role_access_mappings row references a bounded_contexts row that no longer exists",
            );
        Ok(RoleAccessMapping {
            role,
            bounded_context,
            level: access_level_from_str(&self.level),
            can_read_sensitive: self.can_read_sensitive,
            status: role_status_from_str(&self.status),
            created_at: self.created_at,
            revoked_at: self.revoked_at,
        })
    }
}

const ROLE_ACCESS_MAPPING_COLUMNS: &str =
    "role_id, bounded_context, level, can_read_sensitive, status, created_at, revoked_at";

pub async fn insert_role_access_mapping(
    pool: &Pool,
    mapping: &RoleAccessMapping,
) -> crate::error::Result<()> {
    sqlx::query(&format!(
        "INSERT INTO role_access_mappings ({ROLE_ACCESS_MAPPING_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7)"
    ))
    .bind(&mapping.role.id)
    .bind(&mapping.bounded_context.name)
    .bind(access_level_to_str(mapping.level))
    .bind(mapping.can_read_sensitive)
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
pub async fn get_active_role_access_mapping(
    pool: &Pool,
    role_id: &str,
    bounded_context: &str,
) -> crate::error::Result<Option<RoleAccessMapping>> {
    let row: Option<RoleAccessMappingRow> = sqlx::query_as(&format!(
        "SELECT {ROLE_ACCESS_MAPPING_COLUMNS} FROM role_access_mappings \
         WHERE role_id = $1 AND bounded_context = $2 AND status = 'active'"
    ))
    .bind(role_id)
    .bind(bounded_context)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(row) => Ok(Some(row.into_domain(pool).await?)),
        None => Ok(None),
    }
}

/// Every `RoleAccessMapping` this engine currently knows of, active or
/// not - the full-snapshot parameter `grant_role_access_mapping`'s own
/// `existing_mappings` expects (see its doc comment). Same "small,
/// admin-managed, unscoped is fine" reasoning as `list_roles`.
pub async fn list_role_access_mappings(
    pool: &Pool,
) -> crate::error::Result<Vec<RoleAccessMapping>> {
    let rows: Vec<RoleAccessMappingRow> = sqlx::query_as(&format!(
        "SELECT {ROLE_ACCESS_MAPPING_COLUMNS} FROM role_access_mappings"
    ))
    .fetch_all(pool)
    .await?;
    let mut mappings = Vec::with_capacity(rows.len());
    for row in rows {
        mappings.push(row.into_domain(pool).await?);
    }
    Ok(mappings)
}

/// Every currently-*active* `RoleAccessMapping` for one `Role` - the
/// `active_mappings` parameter `revoke_role` expects (see its own doc
/// comment: "as already looked up by the caller").
pub async fn list_active_role_access_mappings_for_role(
    pool: &Pool,
    role_id: &str,
) -> crate::error::Result<Vec<RoleAccessMapping>> {
    let rows: Vec<RoleAccessMappingRow> = sqlx::query_as(&format!(
        "SELECT {ROLE_ACCESS_MAPPING_COLUMNS} FROM role_access_mappings \
         WHERE role_id = $1 AND status = 'active'"
    ))
    .bind(role_id)
    .fetch_all(pool)
    .await?;
    let mut mappings = Vec::with_capacity(rows.len());
    for row in rows {
        mappings.push(row.into_domain(pool).await?);
    }
    Ok(mappings)
}

/// Persists a `revoke_role_access_mapping` (or `revoke_role`'s own
/// cascade) outcome directly, by the same unambiguous `(role_id,
/// bounded_context, status = 'active')` triple `get_active_role_access_mapping`
/// reads by - safe without needing a synthetic id, since at most one row
/// can ever match (see the migration's own partial unique index).
pub async fn revoke_active_role_access_mapping(
    pool: &Pool,
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
    .execute(pool)
    .await?;
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
pub async fn next_sequence(pool: &Pool, bounded_context: &str) -> crate::error::Result<i64> {
    let schema = schema_ident(bounded_context);
    let (next,): (i64,) = sqlx::query_as(&format!(
        "UPDATE {schema}.sequence SET next_value = next_value + 1 RETURNING next_value"
    ))
    .fetch_one(pool)
    .await?;
    Ok(next)
}

/// The highest `sequence` currently committed in a bounded context -
/// `None` when it has no events at all yet. `catch_up_bounded_context`'s
/// own cheap first check every poll tick, so a quiet context (nothing
/// since the last tick) costs one small aggregate query, not a full event
/// reload - `list_events_for_bounded_context_from` only ever runs once
/// this comes back higher than everything that still needs catching up.
pub async fn latest_sequence(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(bounded_context);
    let (max,): (Option<i64>,) =
        sqlx::query_as(&format!("SELECT MAX(sequence) FROM {schema}.events"))
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
/// comments), loaded for real here instead of the in-memory cache the
/// spec's own note describes (still unmodelled - see `event_store`'s
/// module doc comment).
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
    let rows: Vec<EventRow> = sqlx::query_as(&format!(
        "SELECT sequence, payload, metadata_type, metadata_version, metadata_client_id, \
         metadata_created_at, tags, origin_kind, origin_source_content, origin_source_context, \
         origin_command_id FROM {schema}.events WHERE event_type_name = $1 ORDER BY sequence"
    ))
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
pub async fn list_events_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Event>> {
    let bc = get_bounded_context(pool, bounded_context).await?.expect(
        "list_events_for_bounded_context: bounded_context row must exist for any event referencing it",
    );

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(&format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events ORDER BY sequence"
    ))
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
/// shape (still used as-is by `process_command`'s consistency-boundary
/// resolution - unrelated, unchanged). Same per-row resolution as that
/// function, just with a `WHERE` clause and a starting point.
pub async fn list_events_for_bounded_context_from(
    pool: &Pool,
    bounded_context: &str,
    after_sequence: i64,
) -> crate::error::Result<Vec<Event>> {
    let bc = get_bounded_context(pool, bounded_context).await?.expect(
        "list_events_for_bounded_context_from: bounded_context row must exist for any event referencing it",
    );

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(&format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         WHERE sequence > $1 ORDER BY sequence"
    ))
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

/// One `Event` by its own `sequence` - `InspectEvent`'s own lookup key
/// (`context event: Event`), added propagating `skilj-graphql`'s
/// `EventQuery` resolver (Phase 3). `None` when no event in this bounded
/// context has that sequence.
pub async fn get_event_by_sequence(
    pool: &Pool,
    bounded_context: &str,
    sequence: i64,
) -> crate::error::Result<Option<Event>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };

    let schema = schema_ident(bounded_context);
    let Some(row): Option<EventRowAnyType> = sqlx::query_as(&format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events WHERE sequence = $1"
    ))
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

/// `command_id` must be `Some` exactly when `event.origin` is
/// `CommandTriggered`, and `None` otherwise - the caller's own
/// `insert_command(pool, &result.command)` (returning the new row's id)
/// runs first for a `ProcessCommandResult`, then this is called once per
/// produced `Event` with that same id.
pub async fn insert_event<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    event: &Event,
    command_id: Option<i64>,
) -> crate::error::Result<()> {
    debug_assert!(
        event.encryption_keys.is_empty(),
        "insert_event: EncryptionKey persistence isn't wired yet - protect_sensitive_fields' \
         non-empty branch is still todo!(), so no caller of this function can produce one"
    );
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
    sqlx::query(&format!(
        "INSERT INTO {schema}.events (sequence, event_type_name, payload, metadata_type, \
         metadata_version, metadata_client_id, metadata_created_at, tags, encryption_keys, \
         origin_kind, origin_source_content, origin_source_context, origin_command_id) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
    ))
    .bind(event.sequence)
    .bind(&event.event_type.name)
    .bind(&event.payload)
    .bind(&event.metadata.r#type)
    .bind(event.metadata.version)
    .bind(&event.metadata.client_id)
    .bind(event.metadata.created_at)
    .bind(Json(&event.tags))
    .bind(Json(Vec::<serde_json::Value>::new()))
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
pub async fn insert_event_and_update_sync_projections(
    pool: &Pool,
    event: &Event,
    command_id: Option<i64>,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
) -> crate::error::Result<()> {
    let bounded_context = &event.bounded_context.name;
    let schema = schema_ident(bounded_context);

    // Metadata only - read before opening the transaction, the same
    // "small, admin-managed list, not worth locking" treatment
    // `list_projections_for_bounded_context`'s own callers already give
    // it elsewhere. Which projections exist and are `sync` doesn't
    // change mid-request.
    let sync_projections: Vec<_> = list_projections_for_bounded_context(pool, bounded_context)
        .await?
        .into_iter()
        .filter(|p| p.sync)
        .collect();

    let mut tx = pool.begin().await?;
    insert_event(&mut *tx, event, command_id).await?;

    for projection in &sync_projections {
        let (current_state,): (String,) = sqlx::query_as(&format!(
            "SELECT state FROM {schema}.projection_state WHERE projection_name = $1 FOR UPDATE"
        ))
        .bind(&projection.name)
        .fetch_one(&mut *tx)
        .await?;

        let new_state =
            match dispatcher.project(bounded_context, &projection.name, &current_state, event) {
                Some(result) => result?,
                None => current_state,
            };

        sqlx::query(&format!(
            "UPDATE {schema}.projection_state SET state = $1, updated_at = now() \
             WHERE projection_name = $2"
        ))
        .bind(&new_state)
        .bind(&projection.name)
        .execute(&mut *tx)
        .await?;

        sqlx::query(&format!(
            "UPDATE {schema}.projections SET caught_up_to = $1 WHERE name = $2"
        ))
        .bind(event.sequence)
        .bind(&projection.name)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
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
pub async fn catch_up_bounded_context(
    pool: &Pool,
    bounded_context: &str,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let latest = latest_sequence(pool, bounded_context).await?.unwrap_or(-1);

    let all_projections = list_projections_for_bounded_context(pool, bounded_context).await?;
    let async_projections: Vec<_> = all_projections.iter().filter(|p| !p.sync).collect();
    let mut building_rebuilds = Vec::new();
    for projection in &all_projections {
        if let Some(rebuild) =
            get_projection_rebuild(pool, bounded_context, &projection.name).await?
        {
            if rebuild.status == ProjectionRebuildStatus::Building {
                building_rebuilds.push(rebuild);
            }
        }
    }

    if async_projections.is_empty() && building_rebuilds.is_empty() {
        return Ok(());
    }

    for rebuild in &building_rebuilds {
        if rebuild.caught_up_to.is_none() {
            sqlx::query(&format!(
                "DELETE FROM {schema}.projection_rebuild_state WHERE projection_name = $1"
            ))
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
            let existing: Option<(String,)> = sqlx::query_as(&format!(
                "SELECT state FROM {schema}.projection_state WHERE projection_name = $1 FOR UPDATE"
            ))
            .bind(&projection.name)
            .fetch_optional(&mut *tx)
            .await?;
            let Some((current_state,)) = existing else {
                // Never seeded - either reconciliation for this projection
                // hasn't run in this process yet, or it was registered
                // purely via GraphQL with no compiled counterpart to seed
                // it (see `seed_projection_state`'s own callers). Nothing
                // to fold into; retried every subsequent tick.
                continue;
            };

            let new_state = match dispatcher.project(
                bounded_context,
                &projection.name,
                &current_state,
                event,
            ) {
                Some(result) => result?,
                None => current_state,
            };

            sqlx::query(&format!(
                "UPDATE {schema}.projection_state SET state = $1, updated_at = now() \
                 WHERE projection_name = $2"
            ))
            .bind(&new_state)
            .bind(&projection.name)
            .execute(&mut *tx)
            .await?;

            sqlx::query(&format!(
                "UPDATE {schema}.projections SET caught_up_to = $1 WHERE name = $2"
            ))
            .bind(event.sequence)
            .bind(&projection.name)
            .execute(&mut *tx)
            .await?;
        }

        for rebuild in &building_rebuilds {
            if rebuild.caught_up_to.unwrap_or(-1) >= event.sequence {
                continue;
            }
            let existing: Option<(String,)> = sqlx::query_as(&format!(
                "SELECT state FROM {schema}.projection_rebuild_state WHERE projection_name = $1 \
                 FOR UPDATE"
            ))
            .bind(&rebuild.projection.name)
            .fetch_optional(&mut *tx)
            .await?;
            let current_state = match existing {
                Some((s,)) => s,
                None => dispatcher
                    .default_state(bounded_context, &rebuild.projection.name)
                    .unwrap_or_else(|| "{}".to_string()),
            };

            let new_state = match dispatcher.project(
                bounded_context,
                &rebuild.projection.name,
                &current_state,
                event,
            ) {
                Some(result) => result?,
                None => current_state,
            };

            sqlx::query(&format!(
                "INSERT INTO {schema}.projection_rebuild_state (projection_name, state, updated_at) \
                 VALUES ($1, $2, now()) \
                 ON CONFLICT (projection_name) DO UPDATE SET state = EXCLUDED.state, updated_at = now()"
            ))
            .bind(&rebuild.projection.name)
            .bind(&new_state)
            .execute(&mut *tx)
            .await?;

            sqlx::query(&format!(
                "UPDATE {schema}.projection_rebuilds SET caught_up_to = $1 WHERE projection_name = $2"
            ))
            .bind(event.sequence)
            .bind(&rebuild.projection.name)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
    }

    for rebuild in &building_rebuilds {
        let current = get_projection_rebuild(pool, bounded_context, &rebuild.projection.name)
            .await?
            .expect("a building rebuild this function just loaded can't have vanished mid-tick");
        if current.caught_up_to.unwrap_or(-1) == latest {
            promote_projection_rebuild(pool, bounded_context, &rebuild.projection.name).await?;
        }
    }

    Ok(())
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
pub async fn promote_projection_rebuild(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;

    let rebuild_row: ProjectionRebuildRow = sqlx::query_as(&format!(
        "SELECT {PROJECTION_REBUILD_COLUMNS} FROM {schema}.projection_rebuilds \
         WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .fetch_one(&mut *tx)
    .await?;

    sqlx::query(&format!(
        "UPDATE {schema}.projections SET schema = $1, schema_version = $2, sync = $3, \
         caught_up_to = $4 WHERE name = $5"
    ))
    .bind(&rebuild_row.schema)
    .bind(rebuild_row.schema_version)
    .bind(rebuild_row.sync)
    .bind(rebuild_row.caught_up_to)
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;

    sqlx::query(&format!(
        "DELETE FROM {schema}.projection_consumed_event_types WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;
    sqlx::query(&format!(
        "INSERT INTO {schema}.projection_consumed_event_types (projection_name, event_type_name) \
         SELECT projection_name, event_type_name \
         FROM {schema}.projection_rebuild_consumed_event_types WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;

    let state_row: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT state FROM {schema}.projection_rebuild_state WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((state,)) = state_row {
        sqlx::query(&format!(
            "INSERT INTO {schema}.projection_state (projection_name, state, updated_at) \
             VALUES ($1, $2, now()) \
             ON CONFLICT (projection_name) DO UPDATE SET state = EXCLUDED.state, updated_at = now()"
        ))
        .bind(projection_name)
        .bind(&state)
        .execute(&mut *tx)
        .await?;
    }
    // else: this rebuild's dispatcher never resolved even once (see this
    // function's own doc comment) - nothing to copy; the live
    // projection_state row, if any, is left as it was rather than
    // clobbering real content with nothing.

    sqlx::query(&format!(
        "DELETE FROM {schema}.projection_rebuild_state WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;
    sqlx::query(&format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;
    sqlx::query(&format!(
        "DELETE FROM {schema}.projection_rebuilds WHERE projection_name = $1"
    ))
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
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
    let columns: Option<AccessTokenColumns> = sqlx::query_as(&format!(
        "SELECT id, kind, secret, status, created_at, revoked_at, event_type_name, \
         command_type_name FROM {schema}.access_tokens WHERE id = $1"
    ))
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
pub async fn revoke_access_token(
    pool: &Pool,
    id: &str,
    revoked_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    let Some(bounded_context) = resolve_token_bounded_context(pool, id).await? else {
        return Ok(());
    };
    let schema = schema_ident(&bounded_context);
    sqlx::query(&format!(
        "UPDATE {schema}.access_tokens SET status = 'revoked', revoked_at = $1 WHERE id = $2"
    ))
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
/// than a dangling index entry pointing at nothing.
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
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(&format!(
        "INSERT INTO {schema}.access_tokens (id, kind, secret, status, created_at, revoked_at, \
         event_type_name) VALUES ($1,$2,$3,$4,$5,$6,$7)"
    ))
    .bind(id)
    .bind(kind.as_str())
    .bind(secret)
    .bind(token_status_to_str(status))
    .bind(created_at)
    .bind(revoked_at)
    .bind(event_type_name)
    .execute(pool)
    .await?;
    insert_token_index(pool, id, bounded_context).await
}

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
    )
    .await
}

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
    )
    .await
}

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
    )
    .await
}

/// See `insert_access_token_row` above - not built on it directly, since
/// a `CommandToken` fills `command_type_name` instead of the
/// `event_type_name` every other variant does (see the migration's own
/// `access_tokens` `CHECK` constraint), and the same insert-row-then-index
/// ordering.
pub async fn insert_command_token(pool: &Pool, token: &CommandToken) -> crate::error::Result<()> {
    let schema = schema_ident(&token.command_type.bounded_context.name);
    sqlx::query(&format!(
        "INSERT INTO {schema}.access_tokens (id, kind, secret, status, created_at, revoked_at, \
         command_type_name) VALUES ($1,$2,$3,$4,$5,$6,$7)"
    ))
    .bind(&token.id)
    .bind(AccessTokenKind::Command.as_str())
    .bind(&token.secret)
    .bind(token_status_to_str(token.status))
    .bind(token.created_at)
    .bind(token.revoked_at)
    .bind(&token.command_type.name)
    .execute(pool)
    .await?;
    insert_token_index(pool, &token.id, &token.command_type.bounded_context.name).await
}

/// `None` when either no token has this `id` at all, or one does but
/// isn't kind `external_event` - callers that need to tell those two
/// apart (for the 401-vs-403 split - see `AccessTokenKind`'s own doc
/// comment) call `access_token_kind` first.
pub async fn get_external_event_token(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<ExternalEventToken>> {
    let Some(row) = fetch_access_token_row(pool, id).await? else {
        return Ok(None);
    };
    if row.columns.kind != "external_event" {
        return Ok(None);
    }
    let event_type_name = row
        .columns
        .event_type_name
        .expect("external_event access_tokens row without event_type_name");
    let event_type = get_event_type(pool, &row.bounded_context, &event_type_name)
        .await?
        .expect("access_tokens row references an event_type that no longer exists");
    Ok(Some(ExternalEventToken {
        id: row.columns.id,
        secret: row.columns.secret,
        status: token_status_from_str(&row.columns.status),
        created_at: row.columns.created_at,
        revoked_at: row.columns.revoked_at,
        event_type,
    }))
}

/// See `get_external_event_token`'s own doc comment - same shape and
/// `None` reasoning.
pub async fn get_direct_creation_token(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<DirectCreationToken>> {
    let Some(row) = fetch_access_token_row(pool, id).await? else {
        return Ok(None);
    };
    if row.columns.kind != "direct_creation" {
        return Ok(None);
    }
    let event_type_name = row
        .columns
        .event_type_name
        .expect("direct_creation access_tokens row without event_type_name");
    let event_type = get_event_type(pool, &row.bounded_context, &event_type_name)
        .await?
        .expect("access_tokens row references an event_type that no longer exists");
    Ok(Some(DirectCreationToken {
        id: row.columns.id,
        secret: row.columns.secret,
        status: token_status_from_str(&row.columns.status),
        created_at: row.columns.created_at,
        revoked_at: row.columns.revoked_at,
        event_type,
    }))
}

/// See `get_external_event_token`'s own doc comment - same shape and
/// `None` reasoning.
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
    let event_type = get_event_type(pool, &row.bounded_context, &event_type_name)
        .await?
        .expect("access_tokens row references an event_type that no longer exists");
    Ok(Some(EventReadToken {
        id: row.columns.id,
        secret: row.columns.secret,
        status: token_status_from_str(&row.columns.status),
        created_at: row.columns.created_at,
        revoked_at: row.columns.revoked_at,
        event_type,
    }))
}

/// See `get_external_event_token`'s own doc comment - same shape and
/// `None` reasoning, resolving `command_type_name` against
/// `command_types` instead of `event_type_name` against `event_types`.
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
pub async fn get_read_cursor(
    pool: &Pool,
    token: &EventReadToken,
) -> crate::error::Result<Option<ReadCursor>> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    let row: Option<ReadCursorRow> = sqlx::query_as(&format!(
        "SELECT ack_mode, sequence, updated_at FROM {schema}.read_cursors WHERE token_id = $1"
    ))
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
    sqlx::query(&format!(
        "INSERT INTO {schema}.read_cursors (token_id, ack_mode, sequence, updated_at) \
         VALUES ($1,$2,$3,$4) \
         ON CONFLICT (token_id) DO UPDATE SET \
            ack_mode = EXCLUDED.ack_mode, sequence = EXCLUDED.sequence, \
            updated_at = EXCLUDED.updated_at"
    ))
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
            sqlx::query(&format!(
                "UPDATE {schema}.read_cursors SET sequence = $1, updated_at = $2 WHERE token_id = $3"
            ))
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
pub async fn record_acknowledgement(
    pool: &Pool,
    token: &EventReadToken,
    sequence: i64,
    updated_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    sqlx::query(&format!(
        "UPDATE {schema}.read_cursors SET sequence = $1, updated_at = $2 WHERE token_id = $3"
    ))
    .bind(sequence)
    .bind(updated_at)
    .bind(&token.id)
    .execute(pool)
    .await?;
    Ok(())
}
