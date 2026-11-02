//! Persistence for secrets rule groups.

use super::*;

// --- Secret / SecretGrant (docs/architecture.md §Secret) ---
//
// A secret version is a row in `{schema}.secret_versions` holding only the
// *ciphertext* (sealed under that version's own `DataKey`, which is itself
// wrapped under the process-wide master key) plus metadata. The value is
// never stored in the clear, so the identical guarantee the inspector
// already relies on for a sensitive field holds here: a raw database dump
// and the inspector both see ciphertext, nothing else. Rotation/revocation
// destroy the version's `EncryptionKey` in the same transaction
// (`destroy_encryption_key_on`), which is real crypto-shredding.
//
// The `secret_grants` table is the only thing that lets a caller past
// `secrets::require_secret_access`; there is deliberately no admin
// override, so an ungranted adapter (or an admin) is refused a fetch.

pub(super) fn secret_versions_table_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {schema}.secret_versions (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            version BIGINT NOT NULL,
            ciphertext TEXT NOT NULL,
            encryption_key_id BIGINT NOT NULL REFERENCES {schema}.encryption_keys (id),
            status TEXT NOT NULL CHECK (status IN ('active', 'rotated', 'revoked')),
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL,
            rotated_at TIMESTAMPTZ,
            revoked_at TIMESTAMPTZ,
            last_read_at TIMESTAMPTZ,
            last_read_by TEXT,
            UNIQUE (name, version)
        )"
    )
}

pub(super) fn secret_grants_table_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {schema}.secret_grants (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            grantee_role_id TEXT NOT NULL REFERENCES public.roles (id),
            status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
            created_at TIMESTAMPTZ NOT NULL,
            revoked_at TIMESTAMPTZ
        )"
    )
}

/// Creates `secret_versions`/`secret_grants` in one bounded context if they
/// are not already there - the same "patched into every bounded context,
/// every startup, no general per-context migration mechanism"
/// (`ensure_private_field_grants_table`'s own doc comment) treatment.
pub async fn ensure_secret_tables(pool: &Pool, bounded_context: &str) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(secret_versions_table_ddl(&schema)))
        .execute(pool)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "secret_versions_unique_active",
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS secret_versions_unique_active \
             ON {schema}.secret_versions (name) WHERE status = 'active'"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "secret_versions_by_name",
        format!(
            "CREATE INDEX IF NOT EXISTS secret_versions_by_name \
             ON {schema}.secret_versions (name, version DESC)"
        ),
    )))
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(secret_grants_table_ddl(&schema)))
        .execute(pool)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(create_index_patch(
        &schema,
        "secret_grants_by_grantee",
        format!(
            "CREATE INDEX IF NOT EXISTS secret_grants_by_grantee \
             ON {schema}.secret_grants (grantee_role_id, status)"
        ),
    )))
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
pub(super) struct SecretVersionRow {
    pub(super) id: i64,
    pub(super) name: String,
    pub(super) version: i64,
    #[allow(dead_code)]
    pub(super) ciphertext: String,
    #[allow(dead_code)]
    pub(super) encryption_key_id: i64,
    pub(super) status: String,
    pub(super) expires_at: Option<DateTime<Utc>>,
    pub(super) created_at: DateTime<Utc>,
    pub(super) rotated_at: Option<DateTime<Utc>>,
    pub(super) revoked_at: Option<DateTime<Utc>>,
    pub(super) last_read_at: Option<DateTime<Utc>>,
    pub(super) last_read_by: Option<String>,
}

pub(super) const SECRET_VERSION_COLUMNS: &str =
    "id, name, version, ciphertext, encryption_key_id, status, \
     expires_at, created_at, rotated_at, revoked_at, last_read_at, last_read_by";

impl SecretVersionRow {
    /// The `Secret` domain value - note it carries no ciphertext field at
    /// all, so the value cannot ride along on a normal query's result even
    /// by accident.
    pub(super) fn to_domain(&self, bounded_context: BoundedContext) -> Secret {
        Secret {
            bounded_context,
            name: self.name.clone(),
            version: self.version,
            status: secret_status_from_str(&self.status),
            expires_at: self.expires_at,
            created_at: self.created_at,
            rotated_at: self.rotated_at,
            revoked_at: self.revoked_at,
            last_read_at: self.last_read_at,
            last_read_by: self.last_read_by.clone(),
        }
    }
}

// Both status mappings fail closed: the CHECK constraints make any other
// string unreachable today, but should one ever appear (a hand edit, a
// later status added without updating this), it reads as the terminal,
// non-fetchable state rather than as `active`.
pub(super) fn secret_status_from_str(s: &str) -> SecretStatus {
    match s {
        "active" => SecretStatus::Active,
        "rotated" => SecretStatus::Rotated,
        _ => SecretStatus::Revoked,
    }
}

pub(super) fn secret_grant_status_from_str(s: &str) -> SecretGrantStatus {
    match s {
        "active" => SecretGrantStatus::Active,
        _ => SecretGrantStatus::Revoked,
    }
}

/// Serializes every mutating operation on one secret (create/rotate/revoke)
/// within a bounded context, across instances - a transaction-scoped
/// advisory lock keyed by the context and name. Without it two concurrent
/// first-creations could both decide version 1 and collide on
/// `(name, version)`; with it they serialize, the second seeing the first's
/// version and rotating it. Released automatically at commit/rollback.
pub(super) async fn acquire_secret_lock(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<()> {
    let key = format!("skilj-secret:{bounded_context}:{name}");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(key)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The active version row. `for_update` takes a row lock (`SELECT ... FOR
/// UPDATE`) so two concurrent rotations of the same secret serialize: the
/// second waits, re-reads under `READ COMMITTED`, finds the row no longer
/// `active`, and provisions the next version instead of colliding on the
/// `(name, version)` unique constraint.
pub(super) async fn get_active_secret_row_on(
    executor: impl sqlx::PgExecutor<'_>,
    bounded_context: &str,
    name: &str,
    for_update: bool,
) -> crate::error::Result<Option<SecretVersionRow>> {
    let schema = schema_ident(bounded_context);
    let lock = if for_update { " FOR UPDATE" } else { "" };
    let row: Option<SecretVersionRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {SECRET_VERSION_COLUMNS} FROM {schema}.secret_versions \
         WHERE name = $1 AND status = 'active'{lock}"
    )))
    .bind(name)
    .fetch_optional(executor)
    .await?;
    Ok(row)
}

/// The active version of one secret, metadata only - a normal query's own
/// result shape. Never the value.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context, name = %name))]
pub async fn get_active_secret(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<Secret>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    Ok(get_active_secret_row_on(pool, bounded_context, name, false)
        .await?
        .map(|row| row.to_domain(bc)))
}

/// Every active secret's metadata in one bounded context - the shape a
/// `secrets` list query returns, `Secret` (metadata) and never
/// `SecretValue`. Ordered by name so the list is stable.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_active_secrets(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Secret>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<SecretVersionRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {SECRET_VERSION_COLUMNS} FROM {schema}.secret_versions \
         WHERE status = 'active' ORDER BY name"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(|row| row.to_domain(bc.clone())).collect())
}

/// Creates a secret (or a new version of one, rotating the previous active
/// version). The value is sealed under a fresh per-version `DataKey` in the
/// `"secret"`/`"{name}#{version}"` namespace and only the ciphertext is
/// stored. If a previous version was active, it is retired (`rotated`) and
/// its key destroyed in the *same* transaction - so an old secret value is
/// unrecoverable even against someone who later reads the whole database
/// with the master key in hand.
///
/// A concurrent rotation of the same secret serializes on the active row's
/// own lock (`get_active_secret_row_on`'s `for_update`): the second waits,
/// finds the version already retired, and provisions the next one rather
/// than colliding on `(name, version)`.
///
/// Takes the value as a [`SecretValue`], so the input side gets the same
/// no-`Debug`/no-serde protection as a fetched value, and the caller's
/// plaintext is wiped when it drops that value.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context, name = %name))]
pub async fn put_secret_value(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
    plaintext: &SecretValue,
    expires_at: Option<DateTime<Utc>>,
    key_service: &KeyService,
    now: DateTime<Utc>,
) -> crate::error::Result<Secret> {
    secrets::validate_secret_name(name).map_err(crate::error::Error::from)?;
    let bc = require_bounded_context(pool, bounded_context).await?;
    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;
    acquire_secret_lock(&mut tx, bounded_context, name).await?;

    // Retire and shred the currently-active version, if there is one. Read
    // with the transaction so the version counter below is computed under
    // the same snapshot.
    if let Some(old) = get_active_secret_row_on(&mut *tx, bounded_context, name, true).await? {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.secret_versions SET status = 'rotated', rotated_at = $1 \
             WHERE id = $2"
        )))
        .bind(now)
        .bind(old.id)
        .execute(&mut *tx)
        .await?;
        let old_subject_value = secrets::subject_value(name, old.version);
        destroy_encryption_key_on(
            &mut *tx,
            &schema,
            secrets::SUBJECT_KEY,
            &old_subject_value,
            now,
        )
        .await?;
    }

    let next_version: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM {schema}.secret_versions WHERE name = $1"
    )))
    .bind(name)
    .fetch_one(&mut *tx)
    .await?;

    let subject_value = secrets::subject_value(name, next_version);
    let (_key, key_id, data_key) = get_or_create_encryption_key_on(
        &mut tx,
        bc.clone(),
        secrets::SUBJECT_KEY,
        &subject_value,
        key_service,
    )
    .await?;
    let ciphertext = encryption::try_encrypt_leaf(&data_key, plaintext.expose())?;

    let row: SecretVersionRow = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.secret_versions \
         (name, version, ciphertext, encryption_key_id, status, expires_at, created_at) \
         VALUES ($1, $2, $3, $4, 'active', $5, $6) \
         RETURNING {SECRET_VERSION_COLUMNS}"
    )))
    .bind(name)
    .bind(next_version)
    .bind(&ciphertext)
    .bind(key_id)
    .bind(expires_at)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(row.to_domain(bc))
}

/// Explicitly revokes the active version of a secret and destroys its key -
/// the "end of life" that rotation also performs, but terminal and
/// deliberate: nothing can fetch this secret again. Returns the
/// now-revoked version.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context, name = %name))]
pub async fn revoke_secret(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<Secret> {
    secrets::validate_secret_name(name).map_err(crate::error::Error::from)?;
    let bc = require_bounded_context(pool, bounded_context).await?;
    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;
    acquire_secret_lock(&mut tx, bounded_context, name).await?;

    let Some(row) = get_active_secret_row_on(&mut *tx, bounded_context, name, true).await? else {
        return Err(secrets::Error::NotActive {
            name: name.to_string(),
        }
        .into());
    };

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.secret_versions SET status = 'revoked', revoked_at = $1 WHERE id = $2"
    )))
    .bind(now)
    .bind(row.id)
    .execute(&mut *tx)
    .await?;
    let subject_value = secrets::subject_value(name, row.version);
    destroy_encryption_key_on(&mut *tx, &schema, secrets::SUBJECT_KEY, &subject_value, now).await?;

    tx.commit().await?;

    let mut revoked = row.to_domain(bc);
    revoked.status = SecretStatus::Revoked;
    revoked.revoked_at = Some(now);
    Ok(revoked)
}

/// Grants one Role read access to one secret. The grant is what
/// `fetch_secret_value` checks; without it the fetch is refused.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context, name = %name))]
pub async fn grant_secret_access(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
    grantee_role_id: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<SecretGrant> {
    let bc = require_bounded_context(pool, bounded_context).await?;
    let grant = secrets::grant_secret_access(&bc, name, grantee_role_id, now)
        .map_err(crate::error::Error::from)?;
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.secret_grants \
         (id, name, grantee_role_id, status, created_at) VALUES ($1, $2, $3, 'active', $4)"
    )))
    .bind(&grant.id)
    .bind(&grant.name)
    .bind(&grant.grantee_role_id)
    .bind(grant.created_at)
    .execute(pool)
    .await?;
    Ok(grant)
}

/// Ends every active grant of `name` to `grantee_role_id`. Returns how many
/// were revoked (0 is not an error - revoking a grant that was never made,
/// or was already revoked, is idempotent).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context, name = %name))]
pub async fn revoke_secret_access(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
    grantee_role_id: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<u64> {
    let schema = schema_ident(bounded_context);
    let result = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.secret_grants SET status = 'revoked', revoked_at = $1 \
         WHERE name = $2 AND grantee_role_id = $3 AND status = 'active'"
    )))
    .bind(now)
    .bind(name)
    .bind(grantee_role_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

#[derive(sqlx::FromRow)]
pub(super) struct SecretGrantRow {
    pub(super) id: String,
    pub(super) name: String,
    pub(super) grantee_role_id: String,
    pub(super) status: String,
    pub(super) created_at: DateTime<Utc>,
    pub(super) revoked_at: Option<DateTime<Utc>>,
}

pub(super) const SECRET_GRANT_COLUMNS: &str =
    "id, name, grantee_role_id, status, created_at, revoked_at";

impl SecretGrantRow {
    pub(super) fn to_domain(&self, bounded_context: BoundedContext) -> SecretGrant {
        SecretGrant {
            id: self.id.clone(),
            bounded_context,
            name: self.name.clone(),
            grantee_role_id: self.grantee_role_id.clone(),
            status: secret_grant_status_from_str(&self.status),
            created_at: self.created_at,
            revoked_at: self.revoked_at,
        }
    }
}

/// The active grants naming `grantee_role_id` in one bounded context -
/// everything `fetch_secret_value` consults for a caller.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_active_secret_grants_for_grantee(
    pool: &Pool,
    bounded_context: &str,
    grantee_role_id: &str,
) -> crate::error::Result<Vec<SecretGrant>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<SecretGrantRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {SECRET_GRANT_COLUMNS} FROM {schema}.secret_grants \
         WHERE grantee_role_id = $1 AND status = 'active'"
    )))
    .bind(grantee_role_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(|row| row.to_domain(bc.clone())).collect())
}

/// The one place a secret is ever decrypted: an explicit fetch by a caller
/// already granted it.
///
/// Order matters for the leak list: the grant is checked **before** the
/// secret is even looked up, so an ungranted caller (`secret_access_not_granted`)
/// cannot tell a live secret from a missing one - the fetch is a single
/// opaque "no". A granted caller then gets `secret_not_active` if there is
/// no active version (rotated/revoked/never created), or the plaintext if
/// there is. The read is then summarised onto the version's own row
/// (`last_read_at`/`last_read_by`), one `UPDATE`, never an event per fetch.
///
/// `expires_at` is **not** checked here: an active version is returned even
/// once it has expired. That is deliberate - expiry is advisory, the
/// credential's own issuer is what enforces it, and an adapter refreshing
/// an expired OAuth access token typically still needs the stored refresh
/// token (or the expired value itself) to do so. Only rotation and
/// revocation end what a fetch can return; use
/// `secrets::secret_needs_refresh` to act before expiry.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context, name = %name))]
pub async fn fetch_secret_value(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
    caller_role_id: &str,
    key_service: &KeyService,
    now: DateTime<Utc>,
) -> crate::error::Result<SecretValue> {
    secrets::validate_secret_name(name).map_err(crate::error::Error::from)?;
    // Confirms the bounded context exists before anything else; the row
    // itself is not needed here.
    let _ = require_bounded_context(pool, bounded_context).await?;

    let grants =
        list_active_secret_grants_for_grantee(pool, bounded_context, caller_role_id).await?;
    secrets::require_secret_access(name, caller_role_id, &grants)
        .map_err(crate::error::Error::from)?;

    let Some(row) = get_active_secret_row_on(pool, bounded_context, name, false).await? else {
        return Err(secrets::Error::NotActive {
            name: name.to_string(),
        }
        .into());
    };

    let subject_value = secrets::subject_value(name, row.version);
    let Some(data_key) = get_active_data_key_on(
        pool,
        bounded_context,
        secrets::SUBJECT_KEY,
        &subject_value,
        key_service,
    )
    .await?
    else {
        // The active version's key is gone - a torn state that only a raced
        // revoke/rotate or a manual key destroy could produce. Report it as
        // no-active-secret rather than panic; nothing is fetchable.
        return Err(secrets::Error::NotActive {
            name: name.to_string(),
        }
        .into());
    };
    let plaintext = encryption::decrypt_leaf(&data_key, &row.ciphertext)?;

    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.secret_versions SET last_read_at = $1, last_read_by = $2 WHERE id = $3"
    )))
    .bind(now)
    .bind(caller_role_id)
    .bind(row.id)
    .execute(pool)
    .await?;

    Ok(SecretValue::new(plaintext))
}

/// `forgetSubject`'s blind-tag half (docs/architecture.md §205): for every
/// blind tag of `bounded_context` declared `Erasure::WithSubject` with this
/// `subject_key`, removes the tag from every event and command whose
/// `subject_tag` tag is `subject_value` - from `events.tags`, `event_tags`,
/// `commands.consistency_tags` and the items of `commands.consistency_query`
/// - so the value is free again and no hash of it is left. Called before
/// the key is destroyed, so a failure leaves `forgetSubject` retryable.
///
/// One transaction, which also bumps the registrations generation: every
/// instance's event cache refills a window whose registrations stamp
/// changed, so none keeps serving the erased tags. Returns how many events
/// lost a tag.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn forget_subject_in_blind_tags(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
) -> crate::error::Result<u64> {
    ensure_blind_tags_table(pool, bounded_context).await?;
    let schema = schema_ident(bounded_context);
    let erased: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT tag_key, erasure->>'subject_tag' FROM {schema}.blind_tags \
         WHERE erasure->>'kind' = 'with_subject' AND erasure->>'subject_key' = $1 \
         ORDER BY tag_key"
    )))
    .bind(subject_key)
    .fetch_all(pool)
    .await?;
    if erased.is_empty() {
        return Ok(0);
    }
    ensure_registrations_generation(pool, bounded_context).await?;
    let mut tx = pool.begin().await?;
    lock_bounded_context_sequence(&mut tx, bounded_context).await?;
    // A rotation of these tags (docs/architecture.md §209) holds their
    // rows while it rewrites the same events: wait for it here, before
    // touching any, rather than meet it half-way.
    let keys: Vec<&str> = erased.iter().map(|(tag_key, _)| tag_key.as_str()).collect();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT 1 FROM {schema}.blind_tags WHERE tag_key = ANY($1) FOR SHARE"
    )))
    .bind(&keys)
    .execute(&mut *tx)
    .await?;
    let mut events_changed = 0;
    for (tag_key, subject_tag) in &erased {
        let subject = serde_json::json!([{ "key": subject_tag, "value": subject_value }]);
        // The subject's events carrying the blind tag, found through
        // `event_tags` (docs/architecture.md §210), with the tag's index
        // rows to delete exactly.
        let carrying: Vec<(i64, String, Option<String>)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT e.sequence, e.event_type_name, t->>'value' \
                 FROM {schema}.events e, jsonb_array_elements(e.tags) t \
                 WHERE e.sequence = ANY(ARRAY(SELECT s.sequence FROM {schema}.event_tag_refs s \
                    WHERE s.tag_id = (SELECT id FROM {schema}.event_tag_values \
                        WHERE tag_key = $1 AND tag_value = $2))) \
                 AND t->>'key' = $3"
            )))
            .bind(subject_tag)
            .bind(subject_value)
            .bind(tag_key)
            .fetch_all(&mut *tx)
            .await?;
        let mut sequences: Vec<i64> = carrying.iter().map(|(sequence, _, _)| *sequence).collect();
        sequences.sort_unstable();
        sequences.dedup();
        events_changed += sequences.len() as u64;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.events e SET tags = (\
                SELECT coalesce(jsonb_agg(t), '[]'::jsonb) \
                FROM jsonb_array_elements(e.tags) t WHERE t->>'key' <> $2) \
             WHERE e.sequence = ANY($1)"
        )))
        .bind(&sequences)
        .bind(tag_key)
        .execute(&mut *tx)
        .await?;
        let (valued, unvalued): (Vec<_>, Vec<_>) = carrying
            .into_iter()
            .partition(|(_, _, value)| value.is_some());
        let (valued_sequences, (valued_types, values)): (Vec<i64>, (Vec<String>, Vec<String>)) =
            valued
                .into_iter()
                .map(|(sequence, event_type, value)| {
                    (sequence, (event_type, value.unwrap_or_default()))
                })
                .unzip();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {schema}.event_tag_refs et \
             USING unnest($2::text[], $3::text[], $4::bigint[]) AS x(value, event_type, sequence), \
                   {schema}.event_tag_values v \
             WHERE et.tag_id = v.id AND v.tag_key = $1 AND v.tag_value = x.value \
             AND et.event_type_name = x.event_type AND et.sequence = x.sequence"
        )))
        .bind(tag_key)
        .bind(&values)
        .bind(&valued_types)
        .bind(&valued_sequences)
        .execute(&mut *tx)
        .await?;
        if !unvalued.is_empty() {
            let unvalued: Vec<i64> = unvalued.iter().map(|(sequence, _, _)| *sequence).collect();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {schema}.event_tag_refs \
                 WHERE tag_id = (SELECT id FROM {schema}.event_tag_values \
                     WHERE tag_key = $1 AND tag_value IS NULL) AND sequence = ANY($2)"
            )))
            .bind(tag_key)
            .bind(&unvalued)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {schema}.event_tag_values v WHERE tag_key = $1 \
             AND (tag_value = ANY($2) OR tag_value IS NULL) \
             AND NOT EXISTS (SELECT 1 FROM {schema}.event_tag_refs r WHERE r.tag_id = v.id OFFSET 0)"
        )))
        .bind(tag_key)
        .bind(&values)
        .execute(&mut *tx)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.commands c SET \
                consistency_tags = (\
                    SELECT coalesce(jsonb_agg(t), '[]'::jsonb) \
                    FROM jsonb_array_elements(c.consistency_tags) t WHERE t->>'key' <> $2), \
                consistency_query = (\
                    SELECT coalesce(jsonb_agg(jsonb_set(item, '{{tags}}', (\
                        SELECT coalesce(jsonb_agg(t), '[]'::jsonb) \
                        FROM jsonb_array_elements(coalesce(item->'tags', '[]'::jsonb)) t \
                        WHERE t->>'key' <> $2))), '[]'::jsonb) \
                    FROM jsonb_array_elements(c.consistency_query) item) \
             WHERE c.consistency_tags @> $1"
        )))
        .bind(Json(&subject))
        .bind(tag_key)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.registrations_generation SET generation = generation + 1"
    )))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(events_changed)
}
