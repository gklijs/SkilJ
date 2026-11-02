//! Persistence for schema rule groups.

use super::*;

// --- small str <-> enum conversions, shared by more than one entity below ---

pub(super) fn bounded_context_status_to_str(status: BoundedContextStatus) -> &'static str {
    match status {
        BoundedContextStatus::Active => "active",
        BoundedContextStatus::Archived => "archived",
    }
}

pub(super) fn bounded_context_status_from_str(s: &str) -> BoundedContextStatus {
    match s {
        "archived" => BoundedContextStatus::Archived,
        _ => BoundedContextStatus::Active,
    }
}

pub(super) fn role_status_to_str(status: RoleStatus) -> &'static str {
    match status {
        RoleStatus::Active => "active",
        RoleStatus::Revoked => "revoked",
    }
}

pub(super) fn role_status_from_str(s: &str) -> RoleStatus {
    match s {
        "revoked" => RoleStatus::Revoked,
        _ => RoleStatus::Active,
    }
}

pub(super) fn token_status_to_str(status: TokenStatus) -> &'static str {
    match status {
        TokenStatus::Active => "active",
        TokenStatus::Revoked => "revoked",
    }
}

pub(super) fn token_status_from_str(s: &str) -> TokenStatus {
    match s {
        "revoked" => TokenStatus::Revoked,
        _ => TokenStatus::Active,
    }
}

pub(super) fn ack_mode_to_str(mode: AckMode) -> &'static str {
    match mode {
        AckMode::AutoAdvance => "auto_advance",
        AckMode::ManualAck => "manual_ack",
    }
}

pub(super) fn ack_mode_from_str(s: &str) -> AckMode {
    match s {
        "manual_ack" => AckMode::ManualAck,
        _ => AckMode::AutoAdvance,
    }
}

pub(super) fn event_read_start_position_to_str(position: EventReadStartPosition) -> &'static str {
    match position {
        EventReadStartPosition::Beginning => "beginning",
        EventReadStartPosition::Latest => "latest",
        EventReadStartPosition::AtSequence => "at_sequence",
        EventReadStartPosition::AtTime => "at_time",
    }
}

pub(super) fn event_read_start_position_from_str(s: &str) -> EventReadStartPosition {
    match s {
        "latest" => EventReadStartPosition::Latest,
        "at_sequence" => EventReadStartPosition::AtSequence,
        "at_time" => EventReadStartPosition::AtTime,
        _ => EventReadStartPosition::Beginning,
    }
}

pub(super) fn access_level_to_str(level: AccessLevel) -> &'static str {
    match level {
        AccessLevel::Read => "read",
        AccessLevel::Write => "write",
        AccessLevel::Admin => "admin",
    }
}

pub(super) fn access_level_from_str(s: &str) -> AccessLevel {
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
/// time it reaches here. For the quoting to actually be that second line
/// of defence, an embedded `"` is doubled (Postgres's own escape inside a
/// quoted identifier) rather than left to close the identifier early.
pub(crate) fn schema_ident(bounded_context: &str) -> String {
    format!("\"bc_{}\"", bounded_context.replace('"', "\"\""))
}

/// Whether `err` is a bounded context's schema or table gone: deleted
/// since it was listed (docs/architecture.md §157).
pub(crate) fn is_missing_schema(err: &crate::error::Error) -> bool {
    matches!(err, crate::error::Error::Database(e) if e
        .as_database_error()
        .and_then(|d| d.code())
        .is_some_and(|code| code == "42P01" || code == "3F000"))
}

/// `value` as a SQL string literal.
pub(super) fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// `ddl`, adding `columns` to `table` in the bounded context's `schema`,
/// run only when one of them is missing (docs/architecture.md §158).
/// `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` takes its `ACCESS
/// EXCLUSIVE` lock before finding there's nothing to add, so run bare on
/// every startup it queued behind any open read of the table, and every
/// later query queued behind it. The catalog check takes no table lock.
/// A missing table still runs `ddl`, and fails as before.
pub(super) fn add_columns_patch(
    schema: &str,
    table: &str,
    columns: &[&str],
    ddl: String,
) -> String {
    let table = sql_literal(&format!("{schema}.{table}"));
    let missing = columns
        .iter()
        .map(|column| {
            format!(
                "NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass({table}) \
                 AND attname = {} AND attnum > 0 AND NOT attisdropped)",
                sql_literal(column)
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    format!("DO $patch$ BEGIN IF {missing} THEN {ddl}; END IF; END $patch$")
}

/// `ddl`, creating `index` in the bounded context's `schema`, run only
/// when no relation of that name exists: `CREATE INDEX IF NOT EXISTS`
/// takes its `SHARE` lock, which blocks writes, before finding the index
/// already there (docs/architecture.md §158).
pub(super) fn create_index_patch(schema: &str, index: &str, ddl: String) -> String {
    format!(
        "DO $patch$ BEGIN IF to_regclass({}) IS NULL THEN {ddl}; END IF; END $patch$",
        sql_literal(&format!("{schema}.{index}"))
    )
}

/// `ddl`, adding constraint `name` to `table` in the bounded context's
/// `schema`, run only when the table has no constraint of that name
/// (docs/architecture.md §158/§159).
pub(super) fn add_constraint_patch(schema: &str, table: &str, name: &str, ddl: String) -> String {
    format!(
        "DO $patch$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint \
         WHERE conrelid = to_regclass({}) AND conname = {}) THEN {ddl}; END IF; END $patch$",
        sql_literal(&format!("{schema}.{table}")),
        sql_literal(name)
    )
}

// --- per-bounded-context schema provisioning / hard deletion ---

/// Every `CREATE TABLE` a bounded context's own schema needs, in
/// dependency order (each table only ever references one created earlier
/// in this same list). Run once, inside the same transaction as the
/// `bounded_contexts` registry insert - see `insert_bounded_context`.
pub(super) async fn provision_bounded_context_schema(
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
            wrap_nonce BYTEA,
            key_provider TEXT,
            master_key_version INTEGER,
            unreadable_since TIMESTAMPTZ
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
    sqlx::query(sqlx::AssertSqlSafe(encryption_keys_master_key_index_ddl(
        &schema,
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
            consistency_boundary BIGINT,
            consistency_query JSONB NOT NULL DEFAULT '[]'
        )"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX commands_by_created_at ON {schema}.commands (metadata_created_at)"
    )))
    .execute(&mut **tx)
    .await?;
    // issue #18 - backs `event_store::fetch_commands`' own new
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
    ensure_external_message_keys_table(&mut **tx, bounded_context).await?;
    ensure_deadline_cursors_table(&mut **tx, bounded_context).await?;
    // issue #21 - `parked_deliveries`, a brand-new table, so no
    // `ALTER TABLE` patch is needed here the way `cross_context_route_cursors`'
    // own new retry columns need `ensure_cross_context_route_retry_columns`
    // (build()'s own startup loop only, not here - see that function's
    // own doc comment).
    ensure_parked_deliveries_table(&mut **tx, bounded_context).await?;
    // Created here too, not only by `build()`'s own startup migration:
    // `insert_parked_delivery`'s `ON CONFLICT` can't run at all without
    // it, and a bounded context added at runtime (`addBoundedContext`)
    // would otherwise lack it until the next restart. Safe unguarded -
    // the table was only just created, so it has no rows to conflict.
    sqlx::query(sqlx::AssertSqlSafe(parked_deliveries_occurrence_index_ddl(
        &schema,
    )))
    .execute(&mut **tx)
    .await?;
    // issue #20 - `ensure_deadlines_table`'s own two-index shape
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

    // Secrets (docs/architecture.md §Secret) - both tables are created here
    // for a brand-new context and by `ensure_secret_tables` on startup for
    // one provisioned before this pass. `secret_versions` FK-references
    // `encryption_keys` above, so it must come after it.
    sqlx::query(sqlx::AssertSqlSafe(secret_versions_table_ddl(&schema)))
        .execute(&mut **tx)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE UNIQUE INDEX secret_versions_unique_active \
         ON {schema}.secret_versions (name) WHERE status = 'active'"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX secret_versions_by_name ON {schema}.secret_versions (name, version DESC)"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(secret_grants_table_ddl(&schema)))
        .execute(&mut **tx)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX secret_grants_by_grantee ON {schema}.secret_grants (grantee_role_id, status)"
    )))
    .execute(&mut **tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.projections (
            name TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            schema_version BIGINT NOT NULL,
            sync BOOLEAN NOT NULL,
            caught_up_to BIGINT,
            requires_capability TEXT,
            owner_tag_key TEXT
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
    // `as_of_sequence` (issue #25's investigation - see
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
    // issue #25 - `projection_partition_progress`, a brand-new
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
    // `max_payload_bytes`/`warn_payload_bytes`: the bounded context's
    // override of the builder's payload limits, null for none - on this
    // row so the lock every write takes reads them too (§238).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {schema}.sequence (next_value BIGINT NOT NULL, \
         max_payload_bytes BIGINT CHECK (max_payload_bytes >= 0), \
         warn_payload_bytes BIGINT CHECK (warn_payload_bytes >= 0))"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.sequence (next_value) VALUES (-1)"
    )))
    .execute(&mut **tx)
    .await?;

    // Blob references (docs/architecture.md §239).
    sqlx::raw_sql(sqlx::AssertSqlSafe(super::blobs::blob_tables_ddl(&schema)))
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
    // issue #18 - backs `event_store::query_events`/`count_events`'
    // own new correlation_id filter. Same "cold-start/large-history
    // nicety, not a correctness requirement" register as §19's tag index
    // was: both functions already operate on an in-memory slice.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX events_by_correlation_id ON {schema}.events (metadata_correlation_id) \
         WHERE metadata_correlation_id IS NOT NULL"
    )))
    .execute(&mut **tx)
    .await?;
    // No index on `tags`: tag reads go through `event_tags` (§198, §210).
    // §19's GIN index on it, `events_by_tags`, is dropped from bounded
    // contexts provisioned before (`drop_events_by_tags_index`).
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
    // issue #25 (docs/architecture.md §52) -
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
    // issue #98 - the approval policy and proposal tables. Both
    // are brand new, so the same DDL `ensure_command_approval_tables`
    // patches an existing context with works here too.
    sqlx::query(sqlx::AssertSqlSafe(command_approval_policies_table_ddl(
        &schema,
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(command_proposals_table_ddl(&schema)))
        .execute(&mut **tx)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX command_proposals_by_status \
         ON {schema}.command_proposals (status, created_at)"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE UNIQUE INDEX command_proposals_pending_idempotency \
         ON {schema}.command_proposals (command_type_name, metadata_client_id, idempotency_key) \
         WHERE status = 'pending' AND idempotency_key IS NOT NULL"
    )))
    .execute(&mut **tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX command_proposals_page_order \
         ON {schema}.command_proposals (created_at, id)"
    )))
    .execute(&mut **tx)
    .await?;

    // docs/architecture.md §104 - after every table it touches exists.
    add_registration_version_columns(&mut **tx, bounded_context).await?;
    ensure_registrations_generation(&mut **tx, bounded_context).await?;
    ensure_event_tags_index(&mut **tx, bounded_context).await?;
    ensure_blind_tags_table(&mut **tx, bounded_context).await?;
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
pub(super) fn private_field_grants_table_ddl(schema: &str) -> String {
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
            field TEXT,
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
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(&schema, "private_field_grants_by_grantee", format!("CREATE INDEX IF NOT EXISTS private_field_grants_by_grantee ON {schema}.private_field_grants (grantee_role_id, status)"))))
    .execute(pool)
    .await?;
    // Added after the table first shipped; null keeps an existing grant's
    // record-wide reach.
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "private_field_grants",
        &["field"],
        format!("ALTER TABLE {schema}.private_field_grants ADD COLUMN IF NOT EXISTS field TEXT"),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

// --- Command approvals (issue #98) ---

/// `entity CommandApprovalPolicy` - kept in its own table rather than as
/// columns on `command_types`, so adding it changed no `CommandType`
/// literal anywhere. A row's presence is what makes the type require
/// approval; `approver_role`/`deadline_seconds`/`allow_self_approval` are
/// refinements.
pub(super) fn command_approval_policies_table_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {schema}.command_approval_policies (
            command_type_name TEXT PRIMARY KEY REFERENCES {schema}.command_types (name),
            approver_role TEXT,
            deadline_seconds BIGINT CHECK (deadline_seconds IS NULL OR deadline_seconds > 0),
            allow_self_approval BOOLEAN NOT NULL DEFAULT false
        )"
    )
}

/// `entity CommandProposal`. `payload` is encrypted at rest exactly as
/// `commands.payload` is; `metadata_*` mirror the `commands` table's own
/// columns so approval can run the ordinary submission pipeline with the
/// original submitter's `client_id`, correlation and causation ids.
/// `status = 'approving'` is the claim an approver takes before running
/// that pipeline; `command_external_id` is `commands.external_id` once
/// approval produced one.
pub(super) fn command_proposals_table_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {schema}.command_proposals (
            id TEXT PRIMARY KEY,
            command_type_name TEXT NOT NULL REFERENCES {schema}.command_types (name),
            payload TEXT NOT NULL,
            metadata_version BIGINT NOT NULL,
            metadata_client_id TEXT NOT NULL,
            metadata_created_at TIMESTAMPTZ NOT NULL,
            metadata_correlation_id TEXT,
            metadata_causation_id TEXT,
            status TEXT NOT NULL CHECK (status IN \
                ('pending', 'approving', 'approved', 'rejected', 'expired', 'forgotten')),
            idempotency_key TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ,
            decided_at TIMESTAMPTZ,
            decided_by_role_id TEXT REFERENCES public.roles (id),
            rejection_reason TEXT,
            rejection_kind TEXT,
            command_external_id TEXT REFERENCES {schema}.commands (external_id)
        )"
    )
}

pub async fn ensure_command_approval_tables(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    // `command_proposals` references `commands.external_id`, and
    // `command_approval_policies` references `command_types` - both already
    // exist for any context this startup patch runs against. Patched by
    // catalog lookup, not `CREATE ... IF NOT EXISTS`, so an up-to-date
    // context takes no lock at all (docs/architecture.md §158).
    sqlx::query(sqlx::AssertSqlSafe(create_table_patch(
        &schema,
        "command_approval_policies",
        command_approval_policies_table_ddl(&schema),
    )))
    .execute(pool)
    .await?;
    // Added after the table first shipped; `false` keeps an existing policy
    // at the safe default (no self-approval).
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "command_approval_policies",
        &["allow_self_approval"],
        format!(
            "ALTER TABLE {schema}.command_approval_policies \
             ADD COLUMN IF NOT EXISTS allow_self_approval BOOLEAN NOT NULL DEFAULT false"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_table_patch(
        &schema,
        "command_proposals",
        command_proposals_table_ddl(&schema),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "command_proposals_by_status",
        format!(
            "CREATE INDEX command_proposals_by_status \
             ON {schema}.command_proposals (status, created_at)"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "command_proposals_pending_idempotency",
        format!(
            "CREATE UNIQUE INDEX command_proposals_pending_idempotency \
             ON {schema}.command_proposals (command_type_name, metadata_client_id, idempotency_key) \
             WHERE status = 'pending' AND idempotency_key IS NOT NULL"
        ),
    )))
    .execute(pool)
    .await?;
    // `commandProposals`' page order (docs/architecture.md §218).
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "command_proposals_page_order",
        format!(
            "CREATE INDEX command_proposals_page_order \
             ON {schema}.command_proposals (created_at, id)"
        ),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `ddl` (a bare `CREATE TABLE`) run only when `table` does not exist in
/// `schema`: `CREATE TABLE IF NOT EXISTS` takes a lock before finding it
/// already there, the same startup-cost problem as `create_index_patch`.
pub(super) fn create_table_patch(schema: &str, table: &str, ddl: String) -> String {
    format!(
        "DO $patch$ BEGIN IF to_regclass({}) IS NULL THEN {ddl}; END IF; END $patch$",
        sql_literal(&format!("{schema}.{table}"))
    )
}

// --- PrivateFieldGrant ---

/// `commands.id` (the internal `BIGSERIAL`) for a given `Command.id`
/// (`commands.external_id`) - the forward half of the translation
/// `insert_private_field_grant` needs; `get_command_by_id` already is the
/// reverse (see its own doc comment, and the note on `PrivateFieldGrant.
/// command_id` in `access_control` for why a grant stores the internal
/// id rather than the external one).
pub(super) async fn command_internal_id(
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
pub(super) struct PrivateFieldGrantRow {
    pub(super) id: String,
    pub(super) grantor_role_id: String,
    pub(super) grantee_role_id: String,
    pub(super) event_sequence: Option<i64>,
    pub(super) command_id: Option<i64>,
    pub(super) field: Option<String>,
    pub(super) status: String,
    pub(super) created_at: DateTime<Utc>,
    pub(super) revoked_at: Option<DateTime<Utc>>,
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
    pub(super) async fn into_domain(
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
            field: self.field,
            status: token_status_from_str(&self.status),
            created_at: self.created_at,
            revoked_at: self.revoked_at,
        }))
    }
}

pub(super) const PRIVATE_FIELD_GRANT_COLUMNS: &str =
    "id, grantor_role_id, grantee_role_id, event_sequence, command_id, status, created_at, revoked_at, field";

/// Inserts the grant and records nothing: how tests seed one, and a
/// bypass of the access trail (docs/architecture.md §229) for anyone else.
/// Unlike a role mapping, a private-field grant made this way is never
/// baselined, so it has no `PrivateFieldAccessGranted` at all.
/// [`grant_private_field_access_with_trail`] is the write that records it.
#[tracing::instrument(skip_all)]
pub async fn insert_private_field_grant(
    pool: &Pool,
    grant: &PrivateFieldGrant,
) -> crate::error::Result<()> {
    let command_id = private_field_grant_command_id(pool, grant).await?;
    insert_private_field_grant_row(pool, grant, command_id).await
}

/// The internal id of the command a grant names, if it names one.
pub(super) async fn private_field_grant_command_id(
    pool: &Pool,
    grant: &PrivateFieldGrant,
) -> crate::error::Result<Option<i64>> {
    Ok(match grant.command_id.as_deref() {
        Some(external_id) => Some(
            command_internal_id(pool, &grant.bounded_context.name, external_id)
                .await?
                .ok_or_else(crate::error::Error::row_not_found)?,
        ),
        None => None,
    })
}

pub(super) async fn insert_private_field_grant_row<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    grant: &PrivateFieldGrant,
    command_id: Option<i64>,
) -> crate::error::Result<()> {
    let schema = schema_ident(&grant.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.private_field_grants ({PRIVATE_FIELD_GRANT_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)"
    )))
    .bind(&grant.id)
    .bind(&grant.grantor.id)
    .bind(&grant.grantee.id)
    .bind(grant.event_sequence)
    .bind(command_id)
    .bind(token_status_to_str(grant.status))
    .bind(grant.created_at)
    .bind(grant.revoked_at)
    .bind(grant.field.as_deref())
    .execute(executor)
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
    grants_from_rows(pool, &bc, rows).await
}

/// The active grants naming `grantee` in `bounded_context` - all that
/// rendering a record for `grantee` ever consults (`entitled_to_read_private_field`
/// only counts an active grant whose grantee is the reader's own Role).
/// Every event and command read renders with these, a subscription for
/// every event it delivers, so this reads through the
/// `private_field_grants_by_grantee` index rather than the whole table,
/// which is what those reads used to load, a few queries per row
/// (docs/architecture.md §124).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_active_private_field_grants_for_grantee(
    pool: &Pool,
    bounded_context: &str,
    grantee: &Role,
) -> crate::error::Result<Vec<PrivateFieldGrant>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<PrivateFieldGrantRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PRIVATE_FIELD_GRANT_COLUMNS} FROM {schema}.private_field_grants \
         WHERE grantee_role_id = $1 AND status = 'active'"
    )))
    .bind(&grantee.id)
    .fetch_all(pool)
    .await?;
    grants_from_rows(pool, &bc, rows).await
}

/// `PrivateFieldGrantRow::into_domain` for many rows at once: every Role
/// they name in one query, every command they name in one more, instead
/// of two or more queries per row. Rows naming a Role or command that's
/// gone are skipped, as `into_domain` skips them.
pub(super) async fn grants_from_rows(
    pool: &Pool,
    bc: &BoundedContext,
    rows: Vec<PrivateFieldGrantRow>,
) -> crate::error::Result<Vec<PrivateFieldGrant>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut role_ids: Vec<&str> = rows
        .iter()
        .flat_map(|r| [r.grantor_role_id.as_str(), r.grantee_role_id.as_str()])
        .collect();
    role_ids.sort_unstable();
    role_ids.dedup();
    let roles: std::collections::HashMap<String, Role> =
        sqlx::query_as::<_, RoleRow>(sqlx::AssertSqlSafe(format!(
            "SELECT {ROLE_COLUMNS} FROM roles WHERE id = ANY($1)"
        )))
        .bind(&role_ids)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| {
            let role = row.into_domain();
            (role.id.clone(), role)
        })
        .collect();

    let mut command_ids: Vec<i64> = rows.iter().filter_map(|r| r.command_id).collect();
    command_ids.sort_unstable();
    command_ids.dedup();
    let command_external_ids: std::collections::HashMap<i64, String> = if command_ids.is_empty() {
        std::collections::HashMap::new()
    } else {
        let schema = schema_ident(&bc.name);
        sqlx::query_as::<_, (i64, String)>(sqlx::AssertSqlSafe(format!(
            "SELECT id, external_id FROM {schema}.commands WHERE id = ANY($1)"
        )))
        .bind(&command_ids)
        .fetch_all(pool)
        .await?
        .into_iter()
        .collect()
    };

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let command_id = match row.command_id {
                Some(internal_id) => Some(command_external_ids.get(&internal_id)?.clone()),
                None => None,
            };
            Some(PrivateFieldGrant {
                id: row.id,
                bounded_context: bc.clone(),
                grantor: roles.get(&row.grantor_role_id)?.clone(),
                grantee: roles.get(&row.grantee_role_id)?.clone(),
                event_sequence: row.event_sequence,
                command_id,
                field: row.field,
                status: token_status_from_str(&row.status),
                created_at: row.created_at,
                revoked_at: row.revoked_at,
            })
        })
        .collect())
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
/// `true` when an active grant was updated, `false` when there was none
/// (already revoked by a concurrent request): the trail records a
/// revocation only on `true`.
/// Records nothing itself, so a bypass of the access trail like
/// [`insert_private_field_grant`]: [`revoke_private_field_access_with_trail`]
/// is the write that records `PrivateFieldAccessRevoked`.
#[tracing::instrument(skip_all)]
pub async fn update_private_field_grant<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    grant: &PrivateFieldGrant,
) -> crate::error::Result<bool> {
    let schema = schema_ident(&grant.bounded_context.name);
    let done = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.private_field_grants SET status = $1, revoked_at = $2 \
         WHERE id = $3 AND status = 'active'"
    )))
    .bind(token_status_to_str(grant.status))
    .bind(grant.revoked_at)
    .bind(&grant.id)
    .execute(executor)
    .await?;
    Ok(done.rows_affected() == 1)
}

/// `GrantPrivateFieldAccessForEvent`/`ForCommand`'s write: inserts the
/// grant and records `PrivateFieldAccessGranted` in one transaction, so the
/// admin context's log and the grant table cannot disagree (§229). Fails,
/// and grants nothing, when the admin bounded context is missing.
#[tracing::instrument(skip_all)]
pub async fn grant_private_field_access_with_trail(
    pool: &Pool,
    grant: &PrivateFieldGrant,
) -> crate::error::Result<()> {
    let admin = require_admin_bounded_context(pool).await?;
    let command_id = private_field_grant_command_id(pool, grant).await?;
    let mut tx = pool.begin().await?;
    insert_private_field_grant_row(&mut *tx, grant, command_id).await?;
    record_access_event(&mut tx, &admin, AccessChange::private_field_granted(grant)).await?;
    tx.commit().await?;
    Ok(())
}

/// `RevokePrivateFieldAccess`'s write: ends the active grant and records
/// `PrivateFieldAccessRevoked` in one transaction. Nothing is recorded, and
/// `false` is returned, when the grant was no longer active.
#[tracing::instrument(skip_all)]
pub async fn revoke_private_field_access_with_trail(
    pool: &Pool,
    revoked: &PrivateFieldGrant,
) -> crate::error::Result<bool> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    if !update_private_field_grant(&mut *tx, revoked).await? {
        return Ok(false);
    }
    record_access_event(
        &mut tx,
        &admin,
        AccessChange::private_field_revoked(revoked),
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// `idempotency_keys` - a caller-supplied idempotency key on command
/// submission (issue #12), one row per `(command_type_name,
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
    // The `created_at` index serves `delete_expired_idempotency_keys`
    // (docs/architecture.md §87).
    let index = create_index_patch(
        &schema,
        "idempotency_keys_created_at",
        format!(
            "CREATE INDEX IF NOT EXISTS idempotency_keys_created_at \
             ON {schema}.idempotency_keys (created_at)"
        ),
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.idempotency_keys (
            command_type_name TEXT NOT NULL,
            client_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            triggered_event_sequences BIGINT[] NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (command_type_name, client_id, idempotency_key)
        );
        {index}"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// Deletes up to `batch` idempotency keys recorded before `cutoff` in
/// `bounded_context`, returning how many went. `SkiljBuilder`'s
/// retention task calls it until a call deletes fewer than `batch`, so
/// no single statement holds locks on an unbounded number of rows
/// (docs/architecture.md §87). A key gone is simply unseen: a later
/// submission bearing it is processed as new.
pub async fn delete_expired_idempotency_keys(
    pool: &Pool,
    bounded_context: &str,
    cutoff: DateTime<Utc>,
    batch: i64,
) -> crate::error::Result<u64> {
    delete_expired_keys(pool, bounded_context, "idempotency_keys", cutoff, batch).await
}

/// [`delete_expired_idempotency_keys`]' twin for `external_message_keys`
/// (docs/architecture.md §175): the same retention, the same batching.
pub async fn delete_expired_external_message_keys(
    pool: &Pool,
    bounded_context: &str,
    cutoff: DateTime<Utc>,
    batch: i64,
) -> crate::error::Result<u64> {
    delete_expired_keys(
        pool,
        bounded_context,
        "external_message_keys",
        cutoff,
        batch,
    )
    .await
}

/// The batched delete both key tables share. `table` is one of the two
/// names above, never caller input.
pub(super) async fn delete_expired_keys(
    pool: &Pool,
    bounded_context: &str,
    table: &'static str,
    cutoff: DateTime<Utc>,
    batch: i64,
) -> crate::error::Result<u64> {
    let schema = schema_ident(bounded_context);
    let result = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.{table} WHERE ctid IN ( \
             SELECT ctid FROM {schema}.{table} WHERE created_at < $1 LIMIT $2)"
    )))
    .bind(cutoff)
    .bind(batch)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Deletes up to `batch` deadlines in `bounded_context` resolved (fired,
/// cancelled, parked or forgotten) before `cutoff`, returning how many
/// went - `SkiljBuilder::deadline_retention`'s task (docs/architecture.md
/// §163). A pending or firing deadline is never touched. Safe because a
/// schedule's cursor only moves forward: the deleted row's deterministic
/// id (`schedule:sequence`) is never derived again, so nothing relies on
/// its `ON CONFLICT (id) DO NOTHING` once it's gone.
pub async fn delete_expired_deadlines(
    pool: &Pool,
    bounded_context: &str,
    cutoff: DateTime<Utc>,
    batch: i64,
) -> crate::error::Result<u64> {
    let schema = schema_ident(bounded_context);
    let result = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.deadlines WHERE ctid IN ( \
             SELECT ctid FROM {schema}.deadlines \
             WHERE status NOT IN ('pending', 'firing') AND resolved_at < $1 LIMIT $2)"
    )))
    .bind(cutoff)
    .bind(batch)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
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
/// issue #15, only protects against a race *within* one
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
/// (issue #21) - the durable backoff state one route's own
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
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "cross_context_route_cursors",
        &["retry_attempt_count"],
        format!(
            "ALTER TABLE {schema}.cross_context_route_cursors \
         ADD COLUMN IF NOT EXISTS retry_attempt_count INT NOT NULL DEFAULT 0"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "cross_context_route_cursors",
        &["retry_first_failed_at"],
        format!(
            "ALTER TABLE {schema}.cross_context_route_cursors \
         ADD COLUMN IF NOT EXISTS retry_first_failed_at TIMESTAMPTZ"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "cross_context_route_cursors",
        &["retry_next_attempt_at"],
        format!(
            "ALTER TABLE {schema}.cross_context_route_cursors \
         ADD COLUMN IF NOT EXISTS retry_next_attempt_at TIMESTAMPTZ"
        ),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// issue #20 - the durable cursor table `catch_up_schedule_deadline`/
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

/// issue #25's investigation (docs/architecture.md §50/§51) -
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

/// issue #20's own row store - one row per deadline a
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
/// issue #95 - the recurrence columns make one pending row the
/// current head of a chain: `recurrence_schedule` (the 7-field
/// Quartz-dialect cron expression) with `recurrence_policy`
/// (`'skip'`/`'fire_once'`/`'fire_all'`), and the validity window
/// (`recurrence_start_at`/`recurrence_end_at`/`recurrence_valid_until`,
/// all nullable). `recurrence_parent_id` names the chain's first row and
/// `recurrence_sequence` the row's position in it (0 for the first,
/// `NULL` for a one-shot that never recurs), so the next row's
/// deterministic id (`"{parent_id}#r{sequence}"`) is a safe
/// `ON CONFLICT (id) DO NOTHING` no-op on redelivery, the identical
/// property the first row's own `"{schedule_name}:{source_event_sequence}"`
/// id already has. Every column is nullable - a row scheduled before this
/// feature reads as the one-shot it is.
///
/// The DDL itself is shared between `provision_bounded_context_schema`
/// (a brand-new context, run against its own open transaction) and
/// `ensure_deadlines_table` below (an existing one, run against `&Pool`) -
/// one DDL string, not two copies to keep in sync, the identical split
/// `private_field_grants_table_ddl` already uses for the same reason.
pub(super) fn deadlines_table_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {schema}.deadlines (
            id TEXT PRIMARY KEY,
            schedule_name TEXT NOT NULL,
            fire_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ,
            tags JSONB NOT NULL,
            correlation_id TEXT,
            target_bounded_context TEXT NOT NULL,
            target_command_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            resolved_at TIMESTAMPTZ,
            firing_at TIMESTAMPTZ,
            attempt_count INT NOT NULL DEFAULT 0,
            first_failed_at TIMESTAMPTZ,
            next_attempt_at TIMESTAMPTZ,
            last_error TEXT,
            recurrence_schedule TEXT,
            recurrence_policy TEXT CHECK (recurrence_policy IN ('skip', 'fire_once', 'fire_all')),
            recurrence_start_at TIMESTAMPTZ,
            recurrence_end_at TIMESTAMPTZ,
            recurrence_valid_until TIMESTAMPTZ,
            recurrence_parent_id TEXT,
            recurrence_sequence BIGINT
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
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "deadlines",
        &["firing_at"],
        format!("ALTER TABLE {schema}.deadlines ADD COLUMN IF NOT EXISTS firing_at TIMESTAMPTZ"),
    )))
    .execute(pool)
    .await?;
    // issue #97 - a deadline's optional expiry, carried to the
    // fired command's `decide_with`. Nullable, no `DEFAULT`, so every row
    // scheduled before this feature reads as "no expiry".
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "deadlines",
        &["expires_at"],
        format!("ALTER TABLE {schema}.deadlines ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ"),
    )))
    .execute(pool)
    .await?;
    // A failed fire's retry state (docs/architecture.md §115). The
    // defaults mean "never failed", which every existing row is.
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "deadlines",
        &[
            "attempt_count",
            "first_failed_at",
            "next_attempt_at",
            "last_error",
        ],
        format!(
            "ALTER TABLE {schema}.deadlines \
            ADD COLUMN IF NOT EXISTS attempt_count INT NOT NULL DEFAULT 0, \
            ADD COLUMN IF NOT EXISTS first_failed_at TIMESTAMPTZ, \
            ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ, \
            ADD COLUMN IF NOT EXISTS last_error TEXT"
        ),
    )))
    .execute(pool)
    .await?;
    // issue #95 - a recurring chain's own state, one pending row
    // the head. Nullable throughout, so every row scheduled before this
    // feature reads as the one-shot it is.
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "deadlines",
        &[
            "recurrence_schedule",
            "recurrence_policy",
            "recurrence_start_at",
            "recurrence_end_at",
            "recurrence_valid_until",
            "recurrence_parent_id",
            "recurrence_sequence",
        ],
        format!(
            "ALTER TABLE {schema}.deadlines \
            ADD COLUMN IF NOT EXISTS recurrence_schedule TEXT, \
            ADD COLUMN IF NOT EXISTS recurrence_policy TEXT \
                CHECK (recurrence_policy IN ('skip', 'fire_once', 'fire_all')), \
            ADD COLUMN IF NOT EXISTS recurrence_start_at TIMESTAMPTZ, \
            ADD COLUMN IF NOT EXISTS recurrence_end_at TIMESTAMPTZ, \
            ADD COLUMN IF NOT EXISTS recurrence_valid_until TIMESTAMPTZ, \
            ADD COLUMN IF NOT EXISTS recurrence_parent_id TEXT, \
            ADD COLUMN IF NOT EXISTS recurrence_sequence BIGINT"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "deadlines_due",
        format!(
            "CREATE INDEX IF NOT EXISTS deadlines_due ON {schema}.deadlines (fire_at) \
         WHERE status = 'pending'"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "deadlines_tags_gin",
        format!(
            "CREATE INDEX IF NOT EXISTS deadlines_tags_gin ON {schema}.deadlines USING GIN (tags)"
        ),
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

/// `external_message_keys` - the durable state behind
/// `recorded_external_event(adapter, idempotency_key)` in specs/skilj.allium's
/// own `rule CreateExternalEvent` (docs/architecture.md §175): one row per
/// `(adapter_id, message_key)` that created an event, naming that event.
/// The per-message counterpart of `external_message_cursors`, for sources
/// that redeliver out of order. Expires with `idempotency_keys`
/// (`delete_expired_external_message_keys`), hence the `created_at`
/// index. Created on every `build()` and at provisioning, like
/// `ensure_external_message_cursors_table`.
#[tracing::instrument(skip_all)]
pub async fn ensure_external_message_keys_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let index = create_index_patch(
        &schema,
        "external_message_keys_created_at",
        format!(
            "CREATE INDEX IF NOT EXISTS external_message_keys_created_at \
             ON {schema}.external_message_keys (created_at)"
        ),
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.external_message_keys (
            adapter_id TEXT NOT NULL,
            message_key TEXT NOT NULL,
            event_sequence BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (adapter_id, message_key)
        );
        {index}"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// `registrations_generation`: one counter row per bounded context,
/// incremented by triggers whenever an `event_types`/`command_types` row
/// is inserted, deleted or changed. It's what [`events_table_identity`]
/// reads as the [`RegistrationsStamp`], instead of hashing every
/// registration on every cache read (docs/architecture.md §189). An
/// update that only moves `schedule_position`/`last_fired_at` - a
/// scheduled fire - doesn't count, and neither does a re-registration
/// that writes the row back unchanged, as every instance's startup does.
///
/// Created at provisioning and on every `build()`; each step is skipped
/// when already there, since `CREATE TRIGGER` takes a lock that blocks
/// writes to the table (§158), and replacing the function on every start
/// fails when two instances start at once. One multi-statement query, so one
/// transaction: no reader sees the counter before its triggers exist.
///
/// The existence checks and the creates aren't atomic on their own: two
/// instances starting together against a bounded context from before the
/// counter both saw the function missing, and the second `CREATE` failed
/// its `build()` (docs/architecture.md §195). So the query first takes a
/// transaction-scoped advisory lock keyed by the schema; a second instance
/// waits for the first to commit and then finds everything there.
#[tracing::instrument(skip_all)]
pub async fn ensure_registrations_generation<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let triggers = [
        (
            "event_types",
            "event_types_bump_registrations_generation",
            "AFTER INSERT OR DELETE",
            "",
        ),
        (
            "event_types",
            "event_types_change_bumps_registrations_generation",
            "AFTER UPDATE",
            " WHEN ((to_jsonb(OLD) - 'schedule_position' - 'last_fired_at') \
             IS DISTINCT FROM (to_jsonb(NEW) - 'schedule_position' - 'last_fired_at'))",
        ),
        (
            "command_types",
            "command_types_bump_registrations_generation",
            "AFTER INSERT OR DELETE",
            "",
        ),
        (
            "command_types",
            "command_types_change_bumps_registrations_generation",
            "AFTER UPDATE",
            " WHEN (OLD.* IS DISTINCT FROM NEW.*)",
        ),
    ]
    .map(|(table, name, events, when)| {
        format!(
            "DO $patch$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger \
             WHERE tgrelid = to_regclass({}) AND tgname = {}) THEN \
             CREATE TRIGGER {name} {events} ON {schema}.{table} FOR EACH ROW{when} \
             EXECUTE FUNCTION {schema}.bump_registrations_generation(); END IF; END $patch$",
            sql_literal(&format!("{schema}.{table}")),
            sql_literal(name)
        )
    })
    .join(";\n");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "SELECT pg_advisory_xact_lock(hashtext({lock_key})::bigint);
        CREATE TABLE IF NOT EXISTS {schema}.registrations_generation (
            id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
            generation BIGINT NOT NULL
        );
        INSERT INTO {schema}.registrations_generation (generation) VALUES (0)
            ON CONFLICT (id) DO NOTHING;
        DO $patch$ BEGIN IF to_regprocedure({function}) IS NULL THEN
            CREATE FUNCTION {schema}.bump_registrations_generation() RETURNS trigger
                LANGUAGE plpgsql AS $bump$ BEGIN
                    UPDATE {schema}.registrations_generation SET generation = generation + 1;
                    RETURN NULL;
                END $bump$;
        END IF; END $patch$;
        {triggers}",
        function = sql_literal(&format!("{schema}.bump_registrations_generation()")),
        lock_key = sql_literal(&format!("skilj_registrations_generation:{schema}"))
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// Interns exact tag identities in `event_tag_values` and indexes their
/// references in `event_tag_refs` by `(tag_id, event_type_name, sequence)`.
/// `event_tags` remains a compatibility view. Requires PostgreSQL 15 or later.
/// This is what a consistency
/// tag read goes through (docs/architecture.md §198, §210). A GIN index on
/// `events.tags` answered "which events carry this tag", but a query for
/// the latest event of a few types carrying a company's tag still walked
/// every event with that tag; through this index it is a probe per type,
/// however many other events the tag has.
///
/// Filled by a statement-level trigger on `events`, not by skilj's insert
/// paths, so no writer can leave it incomplete - including an instance of
/// an older version during a rolling deploy, which knows nothing about it.
///
/// Created at provisioning, and for a bounded context from before it by
/// [`ensure_bounded_context_current`], under the same kind of advisory
/// lock as [`ensure_registrations_generation`]. The first time, it creates
/// the table, fills it from the existing events and creates the trigger in
/// one transaction, holding a `SHARE ROW EXCLUSIVE` lock on `events`
/// throughout: reads go on, but no event commits between the backfill and
/// the trigger, where it would be indexed by neither. Every later call
/// finds it there and locks nothing (docs/architecture.md §158).
#[tracing::instrument(skip_all)]
pub async fn ensure_event_tags_index<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let sql = include_str!("sql/event_tag_dictionary.sql")
        .replace("@SCHEMA@", &schema)
        .replace(
            "@LOCK_KEY@",
            &sql_literal(&format!("skilj_event_tags:{schema}")),
        );
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(executor)
        .await?;
    Ok(())
}

/// Drops `events_by_tags`, the GIN index on `events.tags` (docs/architecture.md
/// §19), from a bounded context provisioned before tag reads moved to
/// `event_tags` (§210): nothing reads through it, and every event paid for
/// it on insert. Checked in the catalog first, so a startup that finds it
/// gone takes no lock (§158); dropped `CONCURRENTLY`, so writes to the
/// events table go on meanwhile - which can't run in a transaction, hence
/// a statement of its own on the pool.
pub async fn drop_events_by_tags_index(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let index = format!("{schema}.events_by_tags");
    let (present,): (bool,) = sqlx::query_as("SELECT to_regclass($1) IS NOT NULL")
        .bind(&index)
        .fetch_one(pool)
        .await?;
    if present {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP INDEX CONCURRENTLY IF EXISTS {index}"
        )))
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// `blind_tags`: the bounded context's blind-index constraints, one row per
/// tag key (docs/architecture.md §205) - what every instance agrees on
/// about each: its normalizer version, its key fingerprint (§208) and what
/// `forgetSubject` does with it. Created at provisioning, and for a bounded context from before it
/// by [`ensure_bounded_context_current`]; a call that finds it there
/// creates and locks nothing (docs/architecture.md §158).
pub async fn ensure_blind_tags_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DO $patch$ BEGIN IF to_regclass({table}) IS NULL THEN
            CREATE TABLE IF NOT EXISTS {schema}.blind_tags (
                tag_key TEXT PRIMARY KEY,
                normalizer_version BIGINT NOT NULL,
                key_fingerprint TEXT,
                key_generation BIGINT NOT NULL DEFAULT 0,
                erasure JSONB NOT NULL,
                registered_at TIMESTAMPTZ NOT NULL
            );
        END IF; END $patch$",
        table = sql_literal(&format!("{schema}.blind_tags")),
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// The index startup's checks, the rewrap and the retirement check read
/// `encryption_keys` through (docs/architecture.md §230): the active rows
/// by provider and master key version, so "is anything still wrapped by
/// another provider, or below this version" is a few index probes.
pub(super) fn encryption_keys_master_key_index_ddl(schema: &str) -> String {
    format!(
        "CREATE INDEX encryption_keys_master_key ON {schema}.encryption_keys \
         (key_provider, master_key_version, id) WHERE status = 'active'"
    )
}

/// `encryption_keys.key_provider`/`master_key_version`/`unreadable_since`
/// and their index
/// (docs/architecture.md §230), for a bounded context provisioned before
/// them. Rows already there keep `NULL`: wrapped by the legacy raw master
/// key. A call that finds them there takes no table lock (§158).
pub async fn ensure_encryption_key_provider_columns(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "encryption_keys",
        &["key_provider", "master_key_version", "unreadable_since"],
        format!(
            "ALTER TABLE {schema}.encryption_keys \
             ADD COLUMN IF NOT EXISTS key_provider TEXT, \
             ADD COLUMN IF NOT EXISTS master_key_version INTEGER, \
             ADD COLUMN IF NOT EXISTS unreadable_since TIMESTAMPTZ"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "encryption_keys_master_key",
        encryption_keys_master_key_index_ddl(&schema),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Records `declaration` as one of `bounded_context`'s blind tags
/// (docs/architecture.md §205), or checks it against the stored one: the
/// normalizer version must match, and the key fingerprint too when both
/// sides have one (§208; a missing stored one is recorded); the erasure
/// policy is updated, since it only decides what a later `forgetSubject`
/// does. A new key is refused
/// when a registered type already maps it in the clear. Under an advisory
/// lock, so two instances starting together agree.
pub async fn register_blind_tag(
    pool: &Pool,
    bounded_context: &str,
    declaration: &crate::blind::BlindTagDeclaration,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(format!("skilj_blind_tags:{schema}"))
        .execute(&mut *tx)
        .await?;
    let stored: Option<(i64, Option<String>, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT normalizer_version, key_fingerprint, key_generation FROM {schema}.blind_tags \
         WHERE tag_key = $1"
    )))
    .bind(&declaration.tag_key)
    .fetch_optional(&mut *tx)
    .await?;
    // A fingerprint compares only at the generation it was taken at
    // (docs/architecture.md §209); startup checks the stored one.
    let stored = stored.map(|(version, fingerprint, generation)| {
        let comparable = generation == i64::from(declaration.key_generation);
        (version, fingerprint.filter(|_| comparable))
    });
    match stored {
        Some((version, _)) if version != i64::from(declaration.normalizer_version) => {
            return Err(crate::event_store::Error::BlindNormalizerChanged {
                tag_key: declaration.tag_key.clone(),
                stored: version,
                declared: declaration.normalizer_version,
            }
            .into());
        }
        Some((_, Some(stored)))
            if declaration
                .key_fingerprint
                .as_ref()
                .is_some_and(|declared| *declared != stored) =>
        {
            return Err(crate::event_store::Error::BlindIndexKeyChanged {
                bounded_context: bounded_context.to_string(),
                tag_key: declaration.tag_key.clone(),
            }
            .into());
        }
        Some(_) => {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.blind_tags SET erasure = $2, \
                 key_fingerprint = coalesce(key_fingerprint, $3) \
                 WHERE tag_key = $1 AND (key_fingerprint IS NOT NULL OR key_generation = $4)"
            )))
            .bind(&declaration.tag_key)
            .bind(Json(&declaration.erasure))
            .bind(&declaration.key_fingerprint)
            .bind(i64::from(declaration.key_generation))
            .execute(&mut *tx)
            .await?;
        }
        None => {
            let (in_use,): (bool,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT EXISTS (SELECT 1 FROM {schema}.event_types t, \
                    jsonb_array_elements(t.tag_mappings) m \
                    WHERE m->>'key' = $1 AND NOT coalesce((m->>'blind')::boolean, false)) \
                 OR EXISTS (SELECT 1 FROM {schema}.command_types t, \
                    jsonb_array_elements(t.tag_mappings) m \
                    WHERE m->>'key' = $1 AND NOT coalesce((m->>'blind')::boolean, false))"
            )))
            .bind(&declaration.tag_key)
            .fetch_one(&mut *tx)
            .await?;
            if in_use {
                return Err(crate::event_store::Error::BlindTagKeyInUse(
                    declaration.tag_key.clone(),
                )
                .into());
            }
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {schema}.blind_tags \
                 (tag_key, normalizer_version, key_fingerprint, key_generation, erasure, \
                  registered_at) VALUES ($1, $2, $3, $4, $5, $6)"
            )))
            .bind(&declaration.tag_key)
            .bind(i64::from(declaration.normalizer_version))
            .bind(&declaration.key_fingerprint)
            .bind(i64::from(declaration.key_generation))
            .bind(Json(&declaration.erasure))
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

/// `blind_tags`' columns, as `list_blind_tags` reads them.
pub(super) type BlindTagRow = (
    String,
    i64,
    Option<String>,
    i64,
    Json<crate::blind::Erasure>,
);

/// Every blind tag stored for `bounded_context`.
pub async fn list_blind_tags(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<crate::blind::BlindTagDeclaration>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<BlindTagRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT tag_key, normalizer_version, key_fingerprint, key_generation, erasure \
             FROM {schema}.blind_tags ORDER BY tag_key"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(tag_key, version, key_fingerprint, generation, erasure)| {
            crate::blind::BlindTagDeclaration {
                tag_key,
                normalizer_version: u32::try_from(version).unwrap_or(u32::MAX),
                erasure: erasure.0,
                key_fingerprint,
                key_generation: u32::try_from(generation).unwrap_or(u32::MAX),
            }
        })
        .collect())
}

/// Brings this process's view of `bounded_context`'s blind-tag
/// generations up to date, under the bounded context's sequence lock
/// (docs/architecture.md §209) - which a rotation holds while it
/// rewrites, so what's read here can't change until the caller commits.
/// Whether any differed from what this process hashed with: a command
/// whose tags were derived before the lock then has to be derived again.
/// No query at all for a bounded context this process declares no blind
/// tag for.
pub(crate) async fn refresh_blind_generations(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
    blind: &crate::blind::BlindIndex,
) -> crate::error::Result<bool> {
    if !blind.declares_any(bounded_context) {
        return Ok(false);
    }
    let schema = schema_ident(bounded_context);
    let rows: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT tag_key, key_generation FROM {schema}.blind_tags"
    )))
    .fetch_all(&mut *conn)
    .await?;
    let mut changed = false;
    for (tag_key, generation) in rows {
        let generation = u32::try_from(generation).unwrap_or(u32::MAX);
        changed |= blind.observe_generation(bounded_context, &tag_key, generation);
    }
    Ok(changed)
}

/// Rewraps every stored tag of `bounded_context`'s blind tag `tag_key`
/// from generation `from` to `from + 1` (docs/architecture.md §209): in
/// `events.tags`, `event_tags`, the commands' `consistency_tags` and query
/// items, and snapshot rows keyed by it - each value through `wrap`, which
/// needs only the stored tag - and stores the new generation with
/// `fingerprint`, the key chain's up to it. One transaction under the
/// bounded context's sequence lock, so no write lands half-way and a
/// crash leaves the old generation whole; every write to the bounded
/// context waits meanwhile. `false`, changing nothing, when the stored
/// generation isn't `from` - another instance rotated it.
pub async fn rotate_blind_tag(
    pool: &Pool,
    bounded_context: &str,
    tag_key: &str,
    from: u32,
    wrap: &(dyn Fn(&str) -> crate::error::Result<String> + Sync),
    fingerprint: &str,
) -> crate::error::Result<bool> {
    ensure_registrations_generation(pool, bounded_context).await?;
    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;
    lock_bounded_context_sequence(&mut tx, bounded_context).await?;
    let stored: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT key_generation FROM {schema}.blind_tags WHERE tag_key = $1 FOR UPDATE"
    )))
    .bind(tag_key)
    .fetch_optional(&mut *tx)
    .await?;
    if stored != Some((i64::from(from),)) {
        return Ok(false);
    }

    let keyed = serde_json::json!([{ "key": tag_key }]);
    let in_query = serde_json::json!([{ "tags": [{ "key": tag_key }] }]);
    let old: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT tag_value FROM {schema}.event_tags \
            WHERE tag_key = $1 AND tag_value IS NOT NULL \
         UNION SELECT t->>'value' FROM {schema}.commands c, \
            jsonb_array_elements(c.consistency_tags) t \
            WHERE c.consistency_tags @> $2 AND t->>'key' = $1 AND t->>'value' IS NOT NULL \
         UNION SELECT t->>'value' FROM {schema}.commands c, \
            jsonb_array_elements(c.consistency_query) item, \
            jsonb_array_elements(coalesce(item->'tags', '[]'::jsonb)) t \
            WHERE c.consistency_query @> $3 AND t->>'key' = $1 AND t->>'value' IS NOT NULL \
         UNION SELECT tag_value FROM {schema}.snapshots WHERE tag_key = $1"
    )))
    .bind(tag_key)
    .bind(Json(&keyed))
    .bind(Json(&in_query))
    .fetch_all(&mut *tx)
    .await?;
    let old: Vec<String> = old.into_iter().map(|(v,)| v).collect();
    let new = old
        .iter()
        .map(|v| wrap(v))
        .collect::<crate::error::Result<Vec<String>>>()?;

    sqlx::query(
        "CREATE TEMP TABLE skilj_blind_rotation (old TEXT PRIMARY KEY, new TEXT NOT NULL) \
         ON COMMIT DROP",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO skilj_blind_rotation (old, new) SELECT * FROM unnest($1::text[], $2::text[])",
    )
    .bind(&old)
    .bind(&new)
    .execute(&mut *tx)
    .await?;
    // A tag array with each of the key's values replaced, in order.
    let rewrap = |column: &str| {
        format!(
            "(SELECT coalesce(jsonb_agg(CASE WHEN t->>'key' = $1 AND t->>'value' IS NOT NULL \
                THEN jsonb_set(t, '{{value}}', to_jsonb((\
                    SELECT r.new FROM skilj_blind_rotation r WHERE r.old = t->>'value'))) \
                ELSE t END ORDER BY x.ord), '[]'::jsonb) \
              FROM jsonb_array_elements({column}) WITH ORDINALITY AS x(t, ord))"
        )
    };
    sqlx::query(sqlx::AssertSqlSafe(format!(
        // A filter, not an index probe: a rotation rewrites every event the
        // key is on, so reading the table through is what it costs anyway
        // (docs/architecture.md §210).
        "UPDATE {schema}.events e SET tags = {} WHERE e.tags @> $2",
        rewrap("e.tags")
    )))
    .bind(tag_key)
    .bind(Json(&keyed))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.event_tag_values(tag_key, tag_value) \
         SELECT DISTINCT $1, new FROM skilj_blind_rotation ORDER BY 1, 2 \
         ON CONFLICT (tag_key, tag_value) DO NOTHING"
    )))
    .bind(tag_key)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.event_tag_refs refs SET tag_id = fresh.id \
         FROM {schema}.event_tag_values old, skilj_blind_rotation r, \
              {schema}.event_tag_values fresh \
         WHERE refs.tag_id = old.id AND old.tag_key = $1 AND old.tag_value = r.old \
           AND fresh.tag_key = $1 AND fresh.tag_value = r.new"
    )))
    .bind(tag_key)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.event_tag_values v USING skilj_blind_rotation r \
         WHERE v.tag_key = $1 AND v.tag_value = r.old \
           AND NOT EXISTS (SELECT 1 FROM {schema}.event_tag_refs refs WHERE refs.tag_id = v.id OFFSET 0)"
    )))
    .bind(tag_key)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.commands c SET \
            consistency_tags = {}, \
            consistency_query = (SELECT coalesce(jsonb_agg(CASE WHEN q.item ? 'tags' \
                THEN jsonb_set(q.item, '{{tags}}', {}) ELSE q.item END ORDER BY q.ord), \
                '[]'::jsonb) \
              FROM jsonb_array_elements(c.consistency_query) WITH ORDINALITY AS q(item, ord)) \
         WHERE c.consistency_tags @> $2 OR c.consistency_query @> $3",
        rewrap("c.consistency_tags"),
        rewrap("q.item->'tags'"),
    )))
    .bind(tag_key)
    .bind(Json(&keyed))
    .bind(Json(&in_query))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.snapshots s SET tag_value = r.new FROM skilj_blind_rotation r \
         WHERE s.tag_key = $1 AND s.tag_value = r.old"
    )))
    .bind(tag_key)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.blind_tags SET key_generation = $2, key_fingerprint = $3 \
         WHERE tag_key = $1"
    )))
    .bind(tag_key)
    .bind(i64::from(from) + 1)
    .bind(fingerprint)
    .execute(&mut *tx)
    .await?;
    // Every instance's event cache holds the old tags (docs/architecture.md
    // §205's erasure does the same).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.registrations_generation SET generation = generation + 1"
    )))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Records `key_fingerprint` for `bounded_context`'s blind tag `tag_key`
/// when none is stored yet (docs/architecture.md §208): a tenant created
/// by an instance that couldn't compute it. A no-op when one is stored.
pub async fn record_blind_tag_fingerprint(
    pool: &Pool,
    bounded_context: &str,
    tag_key: &str,
    key_fingerprint: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.blind_tags SET key_fingerprint = $2 \
         WHERE tag_key = $1 AND key_fingerprint IS NULL"
    )))
    .bind(tag_key)
    .bind(key_fingerprint)
    .execute(pool)
    .await?;
    Ok(())
}

/// Refuses `tag_mappings` unless each is blind exactly when the bounded
/// context stores a blind tag for its key (docs/architecture.md §205):
/// a key is one constraint for every type that maps it, so a type can't
/// store in the clear what another hashes, nor hash a key no `BlindTag`
/// declares. Checked by the type upserts, whichever surface registers.
pub(super) async fn check_blind_tag_mappings(
    pool: &Pool,
    bounded_context: &str,
    tag_mappings: &[TagMapping],
) -> crate::error::Result<()> {
    if tag_mappings.is_empty() {
        return Ok(());
    }
    // A bounded context an older instance added has no table yet.
    ensure_blind_tags_table(pool, bounded_context).await?;
    let schema = schema_ident(bounded_context);
    let keys: Vec<&str> = tag_mappings.iter().map(|m| m.key.as_str()).collect();
    let blind_keys: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT tag_key FROM {schema}.blind_tags WHERE tag_key = ANY($1)"
    )))
    .bind(&keys)
    .fetch_all(pool)
    .await?;
    let consistent = tag_mappings
        .iter()
        .all(|m| m.blind == blind_keys.iter().any(|(k,)| *k == m.key));
    if !consistent {
        return Err(crate::event_store::Error::BlindTagMismatch.into());
    }
    Ok(())
}

/// `commands.consistency_query`, the query a command was decided with
/// (docs/architecture.md §198), for a bounded context from before it. Its
/// own statement, never in the transaction of
/// [`ensure_event_tags_index`]'s backfill: adding the column locks
/// `commands` against reads too, and held through the backfill it stopped
/// every command read, and every read of a command's events, for as long
/// as that took (docs/architecture.md §200).
#[tracing::instrument(skip_all)]
pub async fn ensure_consistency_query_column<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::raw_sql(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "commands",
        &["consistency_query"],
        format!(
            "ALTER TABLE {schema}.commands ADD COLUMN IF NOT EXISTS consistency_query \
             JSONB NOT NULL DEFAULT '[]'"
        ),
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// Bounded contexts, by name and creation time, this process has run
/// [`ensure_bounded_context_current`] for.
pub(super) type CurrentBoundedContexts =
    std::sync::Mutex<std::collections::HashSet<(String, DateTime<Utc>)>>;

pub(super) fn current_bounded_contexts() -> &'static CurrentBoundedContexts {
    static CURRENT: std::sync::OnceLock<CurrentBoundedContexts> = std::sync::OnceLock::new();
    CURRENT.get_or_init(Default::default)
}

/// Brings a bounded context's schema up to what this version reads and
/// writes - [`ensure_consistency_query_column`], then
/// [`ensure_event_tags_index`], each its own transaction - once per
/// process. `build()` runs it for every bounded context there is; the
/// command path and the background catch-up run it for each they touch,
/// so a bounded context an older instance adds during a rolling deploy,
/// without either, is brought up to date by the first newer instance to
/// meet it rather than failing there until one restarts
/// (docs/architecture.md §200). Keyed by creation time too: a bounded
/// context deleted and added again under the same name is a new schema.
pub async fn ensure_bounded_context_current(
    pool: &Pool,
    bounded_context: &BoundedContext,
) -> crate::error::Result<()> {
    let key = (bounded_context.name.clone(), bounded_context.created_at);
    if current_bounded_contexts()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&key)
    {
        return Ok(());
    }
    make_bounded_context_current(pool, bounded_context).await
}

/// [`ensure_bounded_context_current`] without its once-per-process
/// shortcut: the patches always run, each a catalog lookup when nothing is
/// missing (docs/architecture.md §158). `build()` calls this, so startup
/// brings every bounded context up to date even when something earlier in
/// the process - another instance's background tick - already marked it
/// current and its schema has changed since.
pub async fn make_bounded_context_current(
    pool: &Pool,
    bounded_context: &BoundedContext,
) -> crate::error::Result<()> {
    ensure_projection_owner_tag_key_column(pool, &bounded_context.name).await?;
    ensure_consistency_query_column(pool, &bounded_context.name).await?;
    ensure_event_tags_index(pool, &bounded_context.name).await?;
    ensure_blind_tags_table(pool, &bounded_context.name).await?;
    ensure_encryption_key_provider_columns(pool, &bounded_context.name).await?;
    ensure_payload_limit_columns(pool, &bounded_context.name).await?;
    ensure_blob_tables(pool, &bounded_context.name).await?;
    drop_events_by_tags_index(pool, &bounded_context.name).await?;
    current_bounded_contexts()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert((bounded_context.name.clone(), bounded_context.created_at));
    Ok(())
}

/// The payload-limit override columns on a bounded context's `sequence`
/// row (docs/architecture.md §238), for one provisioned before they
/// existed. Writes read them through `to_jsonb`, so a bounded context
/// without them works - with no override - until this runs.
pub async fn ensure_payload_limit_columns(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "sequence",
        &["max_payload_bytes", "warn_payload_bytes"],
        format!(
            "ALTER TABLE {schema}.sequence \
             ADD COLUMN IF NOT EXISTS max_payload_bytes BIGINT CHECK (max_payload_bytes >= 0), \
             ADD COLUMN IF NOT EXISTS warn_payload_bytes BIGINT CHECK (warn_payload_bytes >= 0)"
        ),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Which registration table [`registration_version`]/
/// [`set_registration_version`] address (docs/architecture.md §104).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegistrationTable {
    EventTypes,
    CommandTypes,
    Projections,
}

impl RegistrationTable {
    pub(super) fn table(self) -> &'static str {
        match self {
            RegistrationTable::EventTypes => "event_types",
            RegistrationTable::CommandTypes => "command_types",
            RegistrationTable::Projections => "projections",
        }
    }
}

pub(super) async fn add_registration_version_columns<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.event_types ADD COLUMN IF NOT EXISTS registered_by_version BIGINT;
         ALTER TABLE {schema}.command_types ADD COLUMN IF NOT EXISTS registered_by_version BIGINT;
         ALTER TABLE {schema}.projections ADD COLUMN IF NOT EXISTS registered_by_version BIGINT"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// The `registered_by_version` column on a bounded context's
/// `event_types`/`command_types`/`projections` - the application version
/// that last registered each row at startup (`SkiljBuilder::
/// application_version`, docs/architecture.md §104). Reconciliation metadata
/// only, never part of the domain types, so nothing but reconciliation
/// reads or writes it. Checked in `information_schema` first so a startup
/// that finds it already there takes no table lock; `provision_bounded_context_schema`
/// adds it for a new bounded context.
pub async fn ensure_registration_version_columns(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let (present,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM information_schema.columns \
         WHERE table_schema = $1 AND column_name = 'registered_by_version' \
         AND table_name IN ('event_types', 'command_types', 'projections')",
    )
    .bind(format!("bc_{bounded_context}"))
    .fetch_one(pool)
    .await?;
    if present < 3 {
        add_registration_version_columns(pool, bounded_context).await?;
    }
    Ok(())
}

/// See [`ensure_registration_version_columns`]. `None` for a row no
/// versioned process has registered (or no such row).
pub async fn registration_version(
    pool: &Pool,
    bounded_context: &str,
    table: RegistrationTable,
    name: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(Option<i64>,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT registered_by_version FROM {schema}.{} WHERE name = $1",
        table.table()
    )))
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(version,)| version))
}

/// See [`ensure_registration_version_columns`].
pub async fn set_registration_version(
    pool: &Pool,
    bounded_context: &str,
    table: RegistrationTable,
    name: &str,
    version: i64,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.{} SET registered_by_version = $1 WHERE name = $2",
        table.table()
    )))
    .bind(version)
    .bind(name)
    .execute(pool)
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
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "projection_state",
        &["owner"],
        format!("ALTER TABLE {schema}.projection_state ADD COLUMN IF NOT EXISTS owner TEXT"),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "projection_rebuild_state",
        &["owner"],
        format!(
            "ALTER TABLE {schema}.projection_rebuild_state ADD COLUMN IF NOT EXISTS owner TEXT"
        ),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `projection_state.as_of_sequence`/`projection_rebuild_state.as_of_sequence`
/// - the real fix behind issue #25's investigation, following
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
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "projection_state",
        &["as_of_sequence"],
        format!(
            "ALTER TABLE {schema}.projection_state \
         ADD COLUMN IF NOT EXISTS as_of_sequence BIGINT NOT NULL DEFAULT -1"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "projection_rebuild_state",
        &["as_of_sequence"],
        format!(
            "ALTER TABLE {schema}.projection_rebuild_state \
         ADD COLUMN IF NOT EXISTS as_of_sequence BIGINT NOT NULL DEFAULT -1"
        ),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `read_cursors.checked_out_at` - the real fix behind issue
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
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "read_cursors",
        &["checked_out_at"],
        format!(
            "ALTER TABLE {schema}.read_cursors ADD COLUMN IF NOT EXISTS checked_out_at TIMESTAMPTZ"
        ),
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
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "event_types",
        &["owner_tag_key"],
        format!("ALTER TABLE {schema}.event_types ADD COLUMN IF NOT EXISTS owner_tag_key TEXT"),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "access_tokens",
        &["scope"],
        format!("ALTER TABLE {schema}.access_tokens ADD COLUMN IF NOT EXISTS scope TEXT"),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "command_types",
        &["owner_tag_key"],
        format!("ALTER TABLE {schema}.command_types ADD COLUMN IF NOT EXISTS owner_tag_key TEXT"),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "snapshots",
        &["owner"],
        format!("ALTER TABLE {schema}.snapshots ADD COLUMN IF NOT EXISTS owner TEXT"),
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
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "event_types",
        &["private_fields"],
        format!("ALTER TABLE {schema}.event_types ADD COLUMN IF NOT EXISTS private_fields JSONB NOT NULL DEFAULT '[]'"),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "command_types",
        &["private_fields"],
        format!("ALTER TABLE {schema}.command_types ADD COLUMN IF NOT EXISTS private_fields JSONB NOT NULL DEFAULT '[]'"),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// `projections.requires_capability` (docs/architecture.md §228,
/// `Projection.requires_capability` in the spec), following
/// `ensure_private_field_columns`'s own pattern for a bounded context
/// provisioned before this column existed. Null for every projection
/// registered before it: nothing was required, and the next
/// registration writes what the compiled projection declares.
pub async fn ensure_projection_requires_capability_column(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "projections",
        &["requires_capability"],
        format!(
            "ALTER TABLE {schema}.projections ADD COLUMN IF NOT EXISTS requires_capability TEXT"
        ),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Patch existing projection registrations with their inspectable owner dimension.
pub async fn ensure_projection_owner_tag_key_column(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "projections",
        &["owner_tag_key"],
        format!("ALTER TABLE {schema}.projections ADD COLUMN IF NOT EXISTS owner_tag_key TEXT"),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Reconcile the owner declaration immediately, including during a staged rebuild.
pub async fn set_projection_owner_tag_key(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
    owner_tag_key: Option<&str>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.projections SET owner_tag_key = $1 WHERE name = $2"
    )))
    .bind(owner_tag_key)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(())
}

/// Set a registered projection's `requires_capability` on its own, for a
/// re-registration that staged a rebuild: who may read applies at once,
/// while the rest of the new definition waits for the rebuild.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn set_projection_requires_capability(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
    requires_capability: Option<&str>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.projections SET requires_capability = $1 WHERE name = $2"
    )))
    .bind(requires_capability)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(())
}

/// `commands.metadata_correlation_id`/`metadata_causation_id`,
/// `events.metadata_correlation_id`/`metadata_causation_id` (
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
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "events",
        &["metadata_correlation_id"],
        format!(
            "ALTER TABLE {schema}.events ADD COLUMN IF NOT EXISTS metadata_correlation_id TEXT"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "events",
        &["metadata_causation_id"],
        format!("ALTER TABLE {schema}.events ADD COLUMN IF NOT EXISTS metadata_causation_id TEXT"),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "commands",
        &["metadata_correlation_id"],
        format!(
            "ALTER TABLE {schema}.commands ADD COLUMN IF NOT EXISTS metadata_correlation_id TEXT"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "commands",
        &["metadata_causation_id"],
        format!(
            "ALTER TABLE {schema}.commands ADD COLUMN IF NOT EXISTS metadata_causation_id TEXT"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "events_by_correlation_id",
        format!(
            "CREATE INDEX IF NOT EXISTS events_by_correlation_id ON {schema}.events \
         (metadata_correlation_id) WHERE metadata_correlation_id IS NOT NULL"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "commands_by_correlation_id",
        format!(
            "CREATE INDEX IF NOT EXISTS commands_by_correlation_id ON {schema}.commands \
         (metadata_correlation_id) WHERE metadata_correlation_id IS NOT NULL"
        ),
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
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "access_tokens",
        &["start_from"],
        format!(
            "ALTER TABLE {schema}.access_tokens \
         ADD COLUMN IF NOT EXISTS start_from TEXT NOT NULL DEFAULT 'beginning'"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "access_tokens",
        &["start_at_sequence"],
        format!(
            "ALTER TABLE {schema}.access_tokens ADD COLUMN IF NOT EXISTS start_at_sequence BIGINT"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(add_columns_patch(
        &schema,
        "access_tokens",
        &["start_at_time"],
        format!(
            "ALTER TABLE {schema}.access_tokens ADD COLUMN IF NOT EXISTS start_at_time TIMESTAMPTZ"
        ),
    )))
    .execute(pool)
    .await?;
    // The check a freshly provisioned `start_from` has, under the name
    // Postgres gives it there. Added on its own, not with the column: a
    // bounded context that got the column from an earlier version of
    // this patch lacks it too (docs/architecture.md §159).
    sqlx::query(sqlx::AssertSqlSafe(add_constraint_patch(
        &schema,
        "access_tokens",
        "access_tokens_start_from_check",
        format!(
            "ALTER TABLE {schema}.access_tokens ADD CONSTRAINT access_tokens_start_from_check \
             CHECK (start_from IN ('beginning', 'latest', 'at_sequence', 'at_time'))"
        ),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Physically, permanently removes a bounded context and everything
/// scoped to it - `DeleteBoundedContext` in specs/skilj.allium. Writes
/// nothing to the access trail, so its grants stay `held` in
/// `capabilityHistory`: production code calls
/// [`hard_delete_bounded_context_with_trail`], and this is for tests and
/// for a caller that records the end itself. One
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

/// [`hard_delete_bounded_context`] that also ends the context's grants in
/// the access trail (docs/architecture.md §229): an `AccessRevoked`
/// (reason `bounded_context_deleted`) for every active mapping and a
/// `PrivateFieldAccessRevoked` for every active private-field grant, with
/// `actor_role_id` as the actor, committed with the deletion. Without them
/// `capabilityHistory` would keep saying a deleted context's grants are
/// held. A context nobody was granted anything in records nothing.
///
/// Locks, in the writers' order (a mapping row, then the admin sequence):
/// the `bounded_contexts` row first, which also holds off a new mapping
/// (its foreign key needs that row), then the active mapping rows, then
/// the private-field grants table, so what is read here is everything
/// there is and nothing can be added before the drop. The schema is
/// dropped before the events are recorded, so the admin sequence lock is
/// held only for those inserts and the commit, not across a large drop.
#[tracing::instrument(skip_all, fields(name = %name))]
pub async fn hard_delete_bounded_context_with_trail(
    pool: &Pool,
    name: &str,
    actor_role_id: &str,
) -> crate::error::Result<()> {
    let admin = require_admin_bounded_context(pool).await?;
    let schema = schema_ident(name);
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT name FROM bounded_contexts WHERE name = $1 FOR UPDATE")
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
    let mapped: Vec<(String,)> = sqlx::query_as(
        "SELECT role_id FROM role_access_mappings \
         WHERE bounded_context = $1 AND status = 'active' ORDER BY role_id FOR UPDATE",
    )
    .bind(name)
    .fetch_all(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE {schema}.private_field_grants IN SHARE ROW EXCLUSIVE MODE"
    )))
    .execute(&mut *tx)
    .await?;
    let private: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, grantee_role_id FROM {schema}.private_field_grants \
         WHERE status = 'active' ORDER BY id"
    )))
    .fetch_all(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM bounded_contexts WHERE name = $1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    for (role_id,) in &mapped {
        record_access_event(
            &mut tx,
            &admin,
            AccessChange::context_ended(role_id, name, actor_role_id),
        )
        .await?;
    }
    for (grant_id, grantee_id) in &private {
        record_access_event(
            &mut tx,
            &admin,
            AccessChange::private_field_ended(grant_id, grantee_id, name, actor_role_id),
        )
        .await?;
    }
    tx.commit().await?;
    notify_registration_changed(pool).await;
    Ok(())
}
