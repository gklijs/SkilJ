//! Persistence for access rule groups.

use super::*;

// --- RoleAccessMapping ---

#[derive(sqlx::FromRow)]
pub(super) struct RoleAccessMappingRow {
    pub(super) role_id: String,
    pub(super) bounded_context: String,
    pub(super) level: String,
    pub(super) can_read_sensitive: bool,
    pub(super) scope: Option<String>,
    pub(super) capabilities: Vec<String>,
    pub(super) delegable: Vec<String>,
    pub(super) status: String,
    pub(super) created_at: DateTime<Utc>,
    pub(super) revoked_at: Option<DateTime<Utc>>,
}

impl RoleAccessMappingRow {
    /// Needs `pool` (unlike every other row type's `into_domain`) since
    /// `RoleAccessMapping` embeds a whole `Role` and a whole
    /// `BoundedContext`, not just their ids - two more queries per row.
    /// Fine for the single-row reads that use it; listings go through
    /// `mappings_from_rows` instead, which batches both
    /// (docs/architecture.md §126).
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
    pub(super) async fn into_domain(
        self,
        pool: &Pool,
    ) -> crate::error::Result<Option<RoleAccessMapping>> {
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
            capabilities: self.capabilities.into_iter().collect(),
            delegable: self.delegable.into_iter().collect(),
            status: role_status_from_str(&self.status),
            created_at: self.created_at,
            revoked_at: self.revoked_at,
        }))
    }
}

pub(super) const ROLE_ACCESS_MAPPING_COLUMNS: &str =
    "role_id, bounded_context, level, can_read_sensitive, scope, capabilities, delegable, \
     status, created_at, revoked_at";

#[tracing::instrument(skip_all)]
pub async fn insert_role_access_mapping<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    mapping: &RoleAccessMapping,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO role_access_mappings ({ROLE_ACCESS_MAPPING_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"
    )))
    .bind(&mapping.role.id)
    .bind(&mapping.bounded_context.name)
    .bind(access_level_to_str(mapping.level))
    .bind(mapping.can_read_sensitive)
    .bind(&mapping.scope)
    .bind(mapping.capabilities.iter().collect::<Vec<_>>())
    .bind(mapping.delegable.iter().collect::<Vec<_>>())
    .bind(role_status_to_str(mapping.status))
    .bind(mapping.created_at)
    .bind(mapping.revoked_at)
    .execute(executor)
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

/// Most capabilities one grant may hold, and most it may delegate
/// (`invariant CapabilitiesAreWellFormed`). Enforced here too, in the
/// statement that adds one, so two concurrent grants cannot both pass a
/// count each read before the other wrote.
pub const MAX_CAPABILITIES_PER_MAPPING: usize = 16;

/// Add `capability` to the active mapping's `capabilities`, in one
/// statement: `true` when it was added, `false` when the mapping is
/// gone or revoked, already holds it, or already holds
/// [`MAX_CAPABILITIES_PER_MAPPING`].
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn add_mapping_capability<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    role_id: &str,
    bounded_context: &str,
    capability: &str,
) -> crate::error::Result<bool> {
    let done = sqlx::query(
        "UPDATE role_access_mappings SET capabilities = array_append(capabilities, $1) \
         WHERE role_id = $2 AND bounded_context = $3 AND status = 'active' \
         AND NOT ($1 = ANY(capabilities)) AND cardinality(capabilities) < $4",
    )
    .bind(capability)
    .bind(role_id)
    .bind(bounded_context)
    .bind(MAX_CAPABILITIES_PER_MAPPING as i32)
    .execute(executor)
    .await?;
    Ok(done.rows_affected() == 1)
}

/// Remove `capability` from the active mapping's `capabilities`: `true`
/// when it was held and is now gone.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn remove_mapping_capability<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    role_id: &str,
    bounded_context: &str,
    capability: &str,
) -> crate::error::Result<bool> {
    let done = sqlx::query(
        "UPDATE role_access_mappings SET capabilities = array_remove(capabilities, $1) \
         WHERE role_id = $2 AND bounded_context = $3 AND status = 'active' \
         AND $1 = ANY(capabilities)",
    )
    .bind(capability)
    .bind(role_id)
    .bind(bounded_context)
    .execute(executor)
    .await?;
    Ok(done.rows_affected() == 1)
}

/// Replace the active mapping's `delegable` set: `true` when the mapping
/// was active and so changed, `false` when the mapping is gone or revoked
/// or the set is over [`MAX_CAPABILITIES_PER_MAPPING`]. The size guard is
/// enforced again here, in the statement itself, so an oversized set is
/// refused even if the caller's own check were bypassed - the same
/// defense-in-depth [`add_mapping_capability`] takes.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn set_mapping_delegable<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    role_id: &str,
    bounded_context: &str,
    delegable: &std::collections::BTreeSet<String>,
) -> crate::error::Result<bool> {
    let done = sqlx::query(
        "UPDATE role_access_mappings SET delegable = $1 \
         WHERE role_id = $2 AND bounded_context = $3 AND status = 'active' \
         AND cardinality($1) <= $4",
    )
    .bind(delegable.iter().collect::<Vec<_>>())
    .bind(role_id)
    .bind(bounded_context)
    .bind(MAX_CAPABILITIES_PER_MAPPING as i32)
    .execute(executor)
    .await?;
    Ok(done.rows_affected() == 1)
}

// --- the access trail (docs/architecture.md §229) ---

/// The `admin` bounded context, which every trail event is written to.
/// An error rather than a skipped write when it is missing: a change that
/// cannot be recorded is not made.
#[tracing::instrument(skip_all)]
pub async fn require_admin_bounded_context(pool: &Pool) -> crate::error::Result<BoundedContext> {
    require_bounded_context(pool, crate::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME).await
}

/// `rule RecordAccessEvent`: appends `change` to the admin bounded
/// context on `tx`, so it commits or rolls back with the change it
/// describes. The admin context's `sequence` row lock is held until `tx`
/// ends, which is what keeps its log gapless. Not broadcast and not put in
/// the event cache: nothing subscribes to these, and a read that misses
/// the cache falls back to the database.
#[tracing::instrument(skip_all, fields(kind = change.kind.name()))]
pub async fn record_access_event(
    tx: &mut Transaction<'_, Postgres>,
    admin: &BoundedContext,
    change: AccessChange,
) -> crate::error::Result<()> {
    let sequence = next_sequence(&mut **tx, &admin.name).await?;
    let event = change.try_into_event(admin, sequence, Utc::now())?;
    insert_event(&mut **tx, &event, None).await
}

/// `GrantRoleAccessMapping`/`CreateBoundedContextFromTemplate`'s write:
/// inserts the mapping and records `AccessGranted` in one transaction.
/// `actor_role_id` is the granting Role, `None` for a grant no Role made
/// (local mode's seed). `insert_role_access_mapping` alone writes no
/// trail; startup's baseline (`baseline_access_events`) covers a row made
/// that way.
#[tracing::instrument(skip_all)]
pub async fn grant_role_access_mapping_with_trail(
    pool: &Pool,
    mapping: &RoleAccessMapping,
    actor_role_id: Option<&str>,
) -> crate::error::Result<()> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    insert_role_access_mapping(&mut *tx, mapping).await?;
    record_access_event(
        &mut tx,
        &admin,
        AccessChange::granted(mapping, actor_role_id),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// `RevokeRoleAccessMapping`'s write: revokes the active mapping and
/// records `AccessRevoked` in one transaction. Nothing is recorded when
/// there was no active mapping to revoke (two requests racing, a stale
/// list): `false`, as for [`change_mapping_capability_with_trail`].
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn revoke_role_access_mapping_with_trail(
    pool: &Pool,
    role_id: &str,
    bounded_context: &str,
    revoked_at: DateTime<Utc>,
    actor_role_id: &str,
) -> crate::error::Result<bool> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    if !revoke_active_role_access_mapping(&mut *tx, role_id, bounded_context, revoked_at).await? {
        return Ok(false);
    }
    record_access_event(
        &mut tx,
        &admin,
        AccessChange::revoked(role_id, bounded_context, actor_role_id, false),
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// The capability mutations' write: applies the grant or revoke and, only
/// when it applied, records the event in the same transaction. `false`
/// when a concurrent change got there first (nothing is recorded).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn change_mapping_capability_with_trail(
    pool: &Pool,
    role_id: &str,
    bounded_context: &str,
    capability: &str,
    grant: bool,
    actor_role_id: &str,
    via_admin: bool,
) -> crate::error::Result<bool> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    let applied = if grant {
        add_mapping_capability(&mut *tx, role_id, bounded_context, capability).await?
    } else {
        remove_mapping_capability(&mut *tx, role_id, bounded_context, capability).await?
    };
    if !applied {
        return Ok(false);
    }
    record_access_event(
        &mut tx,
        &admin,
        AccessChange::capability(
            grant,
            role_id,
            bounded_context,
            actor_role_id,
            capability,
            via_admin,
        ),
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// `SetDelegableCapabilities`' write, with its `DelegableCapabilitiesSet`
/// event in the same transaction.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn set_mapping_delegable_with_trail(
    pool: &Pool,
    role_id: &str,
    bounded_context: &str,
    delegable: &std::collections::BTreeSet<String>,
    actor_role_id: &str,
) -> crate::error::Result<bool> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    if !set_mapping_delegable(&mut *tx, role_id, bounded_context, delegable).await? {
        return Ok(false);
    }
    record_access_event(
        &mut tx,
        &admin,
        AccessChange::delegable_set(role_id, bounded_context, actor_role_id, delegable),
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}

/// Registers the access event types in the admin bounded context
/// (`rule RegisterAccessEventTypes`): each missing one is added, and one
/// already registered gets its schema brought up to date when the library
/// describes it differently now (a release that adds an optional payload
/// field, say), so the stored schema never lags what is written. Nothing
/// else about a registered type is touched, and a restart with nothing new
/// changes nothing. Must run after the per-context startup patches, since
/// the rows carry columns they add.
#[tracing::instrument(skip_all)]
pub async fn register_access_event_types(pool: &Pool) -> crate::error::Result<()> {
    let admin = require_admin_bounded_context(pool).await?;
    for kind in AccessEventKind::ALL {
        let wanted = crate::access_trail::access_event_type(&admin, kind);
        let current = get_event_type(pool, &admin.name, kind.name()).await?;
        let up_to_date = current.as_ref().is_some_and(|current| {
            let parse = |schema: &str| serde_json::from_str::<serde_json::Value>(schema).ok();
            parse(&current.schema).is_some() && parse(&current.schema) == parse(&wanted.schema)
        });
        if !up_to_date {
            upsert_event_type(pool, &wanted).await?;
        }
    }
    Ok(())
}

/// The admin context's `events.origin_kind` check, widened to accept
/// `skilj_recorded`. Only the admin context's table is changed: it is the
/// only one that holds such events, and leaving every other context's
/// check as it was keeps a large events table from being rescanned. Runs
/// the `ALTER` only while the check lacks the value (docs/architecture.md
/// §158).
#[tracing::instrument(skip_all)]
pub async fn ensure_admin_events_origin_kind(pool: &Pool) -> crate::error::Result<()> {
    let schema = schema_ident(crate::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME);
    let mut tx = pool.begin().await?;
    // Instances starting together take turns: the second finds the check
    // already widened. (Without the lock a constraint dropped by the first
    // reads back as NULL to the second's catalog query.)
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('skilj_admin_events_origin_kind'))")
        .execute(&mut *tx)
        .await?;
    let current: Option<(String, Option<String>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT conname::text, pg_get_constraintdef(oid) FROM pg_constraint \
         WHERE conrelid = '{schema}.events'::regclass AND contype = 'c' \
         AND pg_get_constraintdef(oid) LIKE '%origin_kind%'"
    )))
    .fetch_optional(&mut *tx)
    .await?;
    let Some((name, definition)) = current else {
        return Ok(());
    };
    if definition.is_some_and(|definition| definition.contains("skilj_recorded")) {
        return Ok(());
    }
    let name = name.replace('"', "\"\"");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.events DROP CONSTRAINT \"{name}\""
    )))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {schema}.events ADD CONSTRAINT events_origin_kind_check CHECK (\
         origin_kind IN ('external_triggered', 'directly_created', 'command_triggered', \
         'system_triggered', 'skilj_recorded'))"
    )))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// The whole of the trail's startup: the admin events table accepts
/// `skilj_recorded`, the access trail's event types and the master-key
/// trail's (docs/architecture.md §230) are registered, and every grant
/// without history is baselined. Returns how many it baselined. Needs the
/// admin bounded context to exist.
#[tracing::instrument(skip_all)]
pub async fn ensure_access_trail(pool: &Pool) -> crate::error::Result<usize> {
    ensure_admin_events_origin_kind(pool).await?;
    register_access_event_types(pool).await?;
    register_master_key_event_types(pool).await?;
    baseline_access_events(pool).await
}

/// `rule BaselineAccessEvents`: one `AccessBaselined` event for each
/// active mapping that has no access event yet, recording what it holds
/// now. Safe to repeat and safe to run on several instances at once: each
/// baseline re-checks under the admin context's `sequence` row lock, so
/// the loser finds the winner's event and writes nothing. Returns how many
/// it wrote.
#[tracing::instrument(skip_all)]
pub async fn baseline_access_events(pool: &Pool) -> crate::error::Result<usize> {
    let admin = require_admin_bounded_context(pool).await?;
    let schema = schema_ident(&admin.name);
    let mut written = 0;
    for listed in list_role_access_mappings(pool).await? {
        if listed.status != RoleStatus::Active {
            continue;
        }
        let mut tx = pool.begin().await?;
        // The mapping row first, as every writer does (a revoke or a
        // capability change locks the row, then takes the sequence lock):
        // the other order deadlocks. Re-read under that lock, because the
        // listing above is stale by now: a grant revoked or deleted since
        // is not ours to baseline, and one whose capabilities changed
        // must be baselined with what it holds now.
        let row: Option<RoleAccessMappingRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {ROLE_ACCESS_MAPPING_COLUMNS} FROM role_access_mappings \
             WHERE role_id = $1 AND bounded_context = $2 AND status = 'active' FOR UPDATE"
        )))
        .bind(&listed.role.id)
        .bind(&listed.bounded_context.name)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            continue;
        };
        let mapping = RoleAccessMapping {
            level: access_level_from_str(&row.level),
            can_read_sensitive: row.can_read_sensitive,
            scope: row.scope,
            capabilities: row.capabilities.into_iter().collect(),
            delegable: row.delegable.into_iter().collect(),
            ..listed
        };
        // The lock `next_sequence` takes, taken next: the existence check
        // below then cannot race another instance's baseline.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT next_value FROM {schema}.sequence FOR UPDATE"
        )))
        .execute(&mut *tx)
        .await?;
        // Covered means the trail already says the grant is held, not that
        // some event names it: a grant a raw writer made and a capability
        // change then touched has a `CapabilityGranted` and no start, and
        // one raw-re-granted after an `AccessRevoked` folds to not held.
        let records =
            access_event_records(&mut *tx, &mapping.role.id, &mapping.bounded_context.name).await?;
        if fold_access_events(&records, DateTime::<Utc>::MAX_UTC).held {
            continue;
        }
        record_access_event(&mut tx, &admin, AccessChange::baselined(&mapping)).await?;
        tx.commit().await?;
        written += 1;
    }
    Ok(written)
}

/// Every access event for one grant, oldest first (`AccessHistory` folds
/// them).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_access_event_records(
    pool: &Pool,
    role_id: &str,
    bounded_context: &str,
) -> crate::error::Result<Vec<AccessEventRecord>> {
    access_event_records(pool, role_id, bounded_context).await
}

/// [`list_access_event_records`] on any executor, so the baseline can read
/// a grant's history inside the transaction that holds the admin context's
/// sequence lock. One grant's history is one tagged read: the events whose
/// `role` tag is the Role and whose `bounded_context` tag is the context,
/// through `event_tags` (§210) rather than a scan of the payloads. A row
/// that does not parse is an error, not a gap: an audit read that silently
/// dropped a revocation would answer wrongly.
pub(super) async fn access_event_records<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    role_id: &str,
    bounded_context: &str,
) -> crate::error::Result<Vec<AccessEventRecord>> {
    let schema = schema_ident(crate::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME);
    let rows: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT e.event_type_name, e.payload, e.metadata_created_at FROM {schema}.events e \
         WHERE e.sequence IN (SELECT sequence FROM {schema}.event_tags \
             WHERE tag_key = 'role' AND tag_value = $2 AND event_type_name = ANY($1)) \
         AND e.sequence IN (SELECT sequence FROM {schema}.event_tags \
             WHERE tag_key = 'bounded_context' AND tag_value = $3 AND event_type_name = ANY($1)) \
         ORDER BY e.sequence"
    )))
    .bind(
        AccessEventKind::ALL
            .iter()
            .filter(|kind| kind.is_mapping_event())
            .map(|kind| kind.name())
            .collect::<Vec<_>>(),
    )
    .bind(role_id)
    .bind(bounded_context)
    .fetch_all(executor)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(name, payload, created_at)| {
            let kind = AccessEventKind::from_name(&name).ok_or_else(|| {
                sqlx::Error::Decode(format!("{name} is not an access event type").into())
            })?;
            let payload =
                serde_json::from_str(&payload).map_err(|e| sqlx::Error::Decode(e.into()))?;
            Ok(AccessEventRecord {
                kind,
                payload,
                created_at,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?)
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
    mappings_from_rows(pool, rows, None).await
}

/// Every active `RoleAccessMapping` on `bounded_context` - what every
/// GraphQL resolver returning a `BoundedContext` shows with it
/// (`boundedContexts` for every context). It used to list every mapping
/// in the deployment and filter, at two queries per mapping
/// (docs/architecture.md §126).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context.name))]
pub async fn list_active_role_access_mappings_for_bounded_context(
    pool: &Pool,
    bounded_context: &BoundedContext,
) -> crate::error::Result<Vec<RoleAccessMapping>> {
    let rows: Vec<RoleAccessMappingRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ROLE_ACCESS_MAPPING_COLUMNS} FROM role_access_mappings \
         WHERE bounded_context = $1 AND status = 'active' ORDER BY id"
    )))
    .bind(&bounded_context.name)
    .fetch_all(pool)
    .await?;
    mappings_from_rows(pool, rows, Some(bounded_context)).await
}

/// `RoleAccessMappingRow::into_domain` for many rows at once: every Role
/// they name in one query, and each distinct bounded context once
/// (`known`, when the caller already has it, not at all), instead of both
/// per row. A row naming a Role or bounded context that's gone is skipped,
/// as `into_domain` skips it.
pub(super) async fn mappings_from_rows(
    pool: &Pool,
    rows: Vec<RoleAccessMappingRow>,
    known: Option<&BoundedContext>,
) -> crate::error::Result<Vec<RoleAccessMapping>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut role_ids: Vec<&str> = rows.iter().map(|r| r.role_id.as_str()).collect();
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

    let mut contexts: std::collections::HashMap<String, Option<BoundedContext>> =
        std::collections::HashMap::new();
    if let Some(bc) = known {
        contexts.insert(bc.name.clone(), Some(bc.clone()));
    }
    for row in &rows {
        if !contexts.contains_key(&row.bounded_context) {
            let bc = get_bounded_context(pool, &row.bounded_context).await?;
            contexts.insert(row.bounded_context.clone(), bc);
        }
    }

    Ok(rows
        .into_iter()
        .filter_map(|row| {
            Some(RoleAccessMapping {
                role: roles.get(&row.role_id)?.clone(),
                bounded_context: contexts.get(&row.bounded_context)?.clone()?,
                level: access_level_from_str(&row.level),
                can_read_sensitive: row.can_read_sensitive,
                scope: row.scope,
                capabilities: row.capabilities.into_iter().collect(),
                delegable: row.delegable.into_iter().collect(),
                status: role_status_from_str(&row.status),
                created_at: row.created_at,
                revoked_at: row.revoked_at,
            })
        })
        .collect())
}

/// The names of the bounded contexts `role_id` has an active mapping to -
/// just the names, for the per-caller GraphQL schema
/// (docs/architecture.md §138), which needs nothing else.
pub async fn list_accessible_bounded_context_names(
    pool: &Pool,
    role_id: &str,
) -> crate::error::Result<std::collections::BTreeSet<String>> {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT bounded_context FROM role_access_mappings \
         WHERE role_id = $1 AND status = 'active'",
    )
    .bind(role_id)
    .fetch_all(pool)
    .await?;
    Ok(names.into_iter().collect())
}

/// What `role_id` may see of each bounded context it has an active
/// mapping to: the name and the capabilities that mapping holds. For the
/// per-caller GraphQL schema, which leaves a projection out for a caller
/// not holding the capability it requires (docs/architecture.md §228).
pub async fn list_accessible_grants(
    pool: &Pool,
    role_id: &str,
) -> crate::error::Result<Vec<(String, Vec<String>)>> {
    Ok(sqlx::query_as(
        "SELECT bounded_context, capabilities FROM role_access_mappings \
         WHERE role_id = $1 AND status = 'active'",
    )
    .bind(role_id)
    .fetch_all(pool)
    .await?)
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
    mappings_from_rows(pool, rows, None).await
}

/// Persists a `revoke_role_access_mapping` (or `revoke_role`'s own
/// cascade) outcome directly, by the same unambiguous `(role_id,
/// bounded_context, status = 'active')` triple `get_active_role_access_mapping`
/// reads by - safe without needing a synthetic id, since at most one row
/// can ever match (see the migration's own partial unique index). `impl
/// PgExecutor` - same reasoning as `update_role`'s own doc comment.
/// `true` when an active mapping was revoked, `false` when there was none
/// (already revoked by a concurrent request, or never made): the trail
/// records a revocation only on `true`.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn revoke_active_role_access_mapping<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    role_id: &str,
    bounded_context: &str,
    revoked_at: DateTime<Utc>,
) -> crate::error::Result<bool> {
    let done = sqlx::query(
        "UPDATE role_access_mappings SET status = 'revoked', revoked_at = $1 \
         WHERE role_id = $2 AND bounded_context = $3 AND status = 'active'",
    )
    .bind(revoked_at)
    .bind(role_id)
    .bind(bounded_context)
    .execute(executor)
    .await?;
    Ok(done.rows_affected() == 1)
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
    actor_role_id: &str,
) -> crate::error::Result<()> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    update_role(&mut *tx, role).await?;
    for mapping in mappings {
        let revoked_at = mapping
            .revoked_at
            .expect("revoke_role always stamps revoked_at on every mapping it returns");
        if !revoke_active_role_access_mapping(
            &mut *tx,
            &role.id,
            &mapping.bounded_context.name,
            revoked_at,
        )
        .await?
        {
            continue;
        }
        record_access_event(
            &mut tx,
            &admin,
            AccessChange::revoked(&role.id, &mapping.bounded_context.name, actor_role_id, true),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}
