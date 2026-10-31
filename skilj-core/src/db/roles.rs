//! Persistence for roles rule groups.

use super::*;

// --- Role ---

#[derive(sqlx::FromRow)]
pub(super) struct RoleRow {
    pub(super) id: String,
    pub(super) external_subject: String,
    pub(super) name: String,
    pub(super) superadmin: bool,
    pub(super) status: String,
    pub(super) created_at: DateTime<Utc>,
    pub(super) revoked_at: Option<DateTime<Utc>>,
}

impl RoleRow {
    pub(super) fn into_domain(self) -> Role {
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

pub(super) const ROLE_COLUMNS: &str =
    "id, external_subject, name, superadmin, status, created_at, revoked_at";

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

/// `rule CreateSuperadmin`'s `requires: not exists Role{superadmin: true,
/// status: active}` and its `ensures`, as one atomic step
/// (docs/architecture.md §109): under a transaction-scoped advisory lock,
/// re-checks that no active superadmin exists and only then inserts
/// `role`. Returns `false`, inserting nothing, when one does - including one
/// a concurrent claim (another instance, with its own bootstrap secret)
/// committed after the caller's own check. Without the lock and re-check,
/// two concurrent claims both passed and both became superadmins.
pub async fn insert_superadmin_if_none_active(
    pool: &Pool,
    role: &Role,
) -> crate::error::Result<bool> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('skilj_create_superadmin')::bigint)")
        .execute(&mut *tx)
        .await?;
    let (exists,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM roles WHERE superadmin AND status = 'active')",
    )
    .fetch_one(&mut *tx)
    .await?;
    if exists {
        return Ok(false);
    }
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
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(true)
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
/// expect as their own `existing_roles` (see their doc comments). An
/// unscoped list, since a `Role` isn't scoped to a bounded context - but
/// not for per-request paths: every user a deployment registers is a
/// row, so authentication uses `active_roles_by_external_subject`
/// instead (docs/architecture.md §110).
#[tracing::instrument(skip_all)]
pub async fn list_roles(pool: &Pool) -> crate::error::Result<Vec<Role>> {
    let rows: Vec<RoleRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ROLE_COLUMNS} FROM roles"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(RoleRow::into_domain).collect())
}

/// The active `Role` claiming `external_subject`, as the one-element
/// `existing_roles` slice `resolve_role_by_external_subject` takes - what
/// authenticating a request needs, instead of `list_roles`' whole table.
/// At most one row: `roles_unique_active_external_subject` is a unique
/// index on exactly this predicate, and serves the lookup
/// (docs/architecture.md §110).
#[tracing::instrument(skip_all)]
pub async fn active_roles_by_external_subject(
    pool: &Pool,
    external_subject: &str,
) -> crate::error::Result<Vec<Role>> {
    let rows: Vec<RoleRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ROLE_COLUMNS} FROM roles WHERE external_subject = $1 AND status = 'active'"
    )))
    .bind(external_subject)
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
