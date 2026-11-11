//! Persistence for encryption keys rule groups.

use super::*;

// --- EncryptionKey ---

pub(super) fn encryption_key_status_from_str(s: &str) -> EncryptionKeyStatus {
    match s {
        "destroyed" => EncryptionKeyStatus::Destroyed,
        _ => EncryptionKeyStatus::Active,
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct EncryptionKeyRow {
    pub(super) id: i64,
    pub(super) subject_key: String,
    pub(super) subject_value: String,
    pub(super) status: String,
    pub(super) created_at: DateTime<Utc>,
    pub(super) destroyed_at: Option<DateTime<Utc>>,
    pub(super) wrapped_key: Option<Vec<u8>>,
    pub(super) wrap_nonce: Option<Vec<u8>>,
    /// The key provider that wrapped it (docs/architecture.md §230),
    /// `NULL` for one wrapped by the legacy raw master key.
    pub(super) key_provider: Option<String>,
    /// The master key version it is wrapped under, `NULL` for a legacy one.
    pub(super) master_key_version: Option<i32>,
    /// When a rewrap found no configured key unwraps it.
    pub(super) unreadable_since: Option<DateTime<Utc>>,
}

impl EncryptionKeyRow {
    pub(super) fn to_domain(&self, bounded_context: BoundedContext) -> EncryptionKey {
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

pub(super) const ENCRYPTION_KEY_COLUMNS: &str =
    "id, subject_key, subject_value, status, created_at, \
     destroyed_at, wrapped_key, wrap_nonce, key_provider, master_key_version, unreadable_since";

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
///
/// Refuses the secrets namespace (`secrets::is_reserved_subject_key`): an
/// application's declared `SensitiveField` cannot provision or share a
/// secret version's key. A secret version's own key is provisioned by
/// `put_secret_value` through the private `get_or_create_encryption_key_on`.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_or_create_encryption_key(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    key_service: &KeyService,
) -> crate::error::Result<(EncryptionKey, i64, DataKey)> {
    if secrets::is_reserved_subject_key(subject_key) {
        return Err(secrets::Error::ReservedSubjectKey.into());
    }
    let bc = require_bounded_context(pool, bounded_context).await?;
    provision_encryption_key(Db::Pool(pool), bc, subject_key, subject_value, key_service).await
}

/// [`get_or_create_encryption_key`] on a connection the caller holds,
/// with the bounded context already in hand. A command's own post-lock
/// provisioning runs it on the transaction holding the bounded context's
/// `sequence` lock, so it never needs a second pooled connection while
/// holding that lock (docs/architecture.md §117). A key created there
/// becomes durable with the command that needed it, or not at all.
pub(super) async fn get_or_create_encryption_key_on(
    conn: &mut sqlx::PgConnection,
    bc: BoundedContext,
    subject_key: &str,
    subject_value: &str,
    key_service: &KeyService,
) -> crate::error::Result<(EncryptionKey, i64, DataKey)> {
    provision_encryption_key(Db::Conn(conn), bc, subject_key, subject_value, key_service).await
}

/// [`get_or_create_encryption_key_on`] on either a held connection or the
/// pool (see [`Db`]).
pub(super) async fn provision_encryption_key(
    mut db: Db<'_>,
    bc: BoundedContext,
    subject_key: &str,
    subject_value: &str,
    key_service: &KeyService,
) -> crate::error::Result<(EncryptionKey, i64, DataKey)> {
    let bounded_context = bc.name.clone();
    let bounded_context = bounded_context.as_str();
    let schema = schema_ident(bounded_context);

    // docs/architecture.md §107: normally one pass. Another pass only when
    // the insert lost to a concurrent provisioner *and* that key was
    // destroyed (a concurrent `forgetSubject`) before this re-read it -
    // no active key at all, so provisioning again is the right outcome
    // (new data about a subject after its erasure gets a new key). This
    // used to `expect` and panic. Bounded, so a pathological churn is an
    // error, not a hang.
    const ATTEMPTS: usize = 5;
    for _ in 0..ATTEMPTS {
        let existing = get_active_encryption_key_row(
            &mut *db.conn().await?,
            bounded_context,
            subject_key,
            subject_value,
        )
        .await?;
        if let Some(row) = existing {
            let data_key = unwrap_row(&mut db, bounded_context, key_service, &row).await?;
            return Ok((row.to_domain(bc), row.id, data_key));
        }

        // The bounded context's own key, or the default one
        // (docs/architecture.md §230) - read every time, so a key assigned
        // on another instance is used from then on.
        let (key, recorded) = {
            let mut conn = db.conn().await?;
            let key = master_key::bounded_context_key(&mut *conn, bounded_context).await?;
            let recorded = match &key {
                Some(key) => {
                    let name = key_service.provider_for(Some(key))?.name().to_string();
                    master_key::key_recorded(&mut *conn, &name).await?
                }
                None => true,
            };
            (key, recorded)
        };
        let provider = key_service.provider_for(key.as_deref())?;
        // A bounded context's own key that is gone - the context was given
        // another key and this one deleted since the assignment was read -
        // isn't asked to wrap anything (Vault may even create a missing key
        // on encrypt): the next pass reads the assignment again.
        if !recorded {
            continue;
        }
        let (data_key, wrapped) = match key_service.generate_with(provider.as_ref()).await {
            Ok(generated) => generated,
            // The same, found by the provider: try again only when the
            // assignment did change meanwhile.
            Err(err)
                if key.is_some()
                    && master_key::bounded_context_key(&mut *db.conn().await?, bounded_context)
                        .await?
                        != key =>
            {
                tracing::info!(error = %err, "bounded context's key changed while wrapping; again");
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        let now = Utc::now();
        // A bounded context's own key is stored only while its state row
        // is there, taken `FOR SHARE` in the same statement: a deletion of
        // the key waits for this, and one that came first leaves nothing to
        // take, so nothing is inserted and the next pass reads the bounded
        // context's key again (docs/architecture.md §230). The default key
        // is never deleted.
        let guard = if key.is_some() {
            "SELECT 1 FROM encryption_master_keys WHERE provider = $5 FOR SHARE"
        } else {
            "SELECT 1"
        };
        let inserted: Option<EncryptionKeyRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "WITH kept AS ({guard}) \
             INSERT INTO {schema}.encryption_keys \
             (subject_key, subject_value, status, created_at, wrapped_key, key_provider, \
             master_key_version) \
             SELECT $1, $2, 'active', $3, $4, $5, $6 FROM kept \
             ON CONFLICT (subject_key, subject_value) WHERE status = 'active' DO NOTHING \
             RETURNING {ENCRYPTION_KEY_COLUMNS}"
        )))
        .bind(subject_key)
        .bind(subject_value)
        .bind(now)
        .bind(&wrapped.ciphertext)
        .bind(provider.name())
        .bind(version_column(wrapped.version)?)
        .fetch_optional(&mut *db.conn().await?)
        .await?;

        if let Some(row) = inserted {
            return Ok((row.to_domain(bc), row.id, data_key));
        }
        // Lost the race to a concurrent provisioner - the next pass reads
        // the row it created (never the key just generated here, which
        // was never persisted and would silently disagree with it).
    }
    Err(crate::error::Error::row_not_found())
}

/// Where a provisioning or a read runs its SQL (docs/architecture.md §230):
/// on a connection the caller holds - a command's transaction under the
/// bounded context's `sequence` lock - or on the pool, a connection taken
/// for each statement and given back before any key provider is called, so
/// a slow provider doesn't hold pool connections while it answers.
pub(super) enum Db<'c> {
    Conn(&'c mut sqlx::PgConnection),
    Pool(&'c Pool),
}

/// A connection for one statement: the caller's, or one from the pool.
pub(super) enum Held<'a> {
    Borrowed(&'a mut sqlx::PgConnection),
    Owned(sqlx::pool::PoolConnection<Postgres>),
}

impl std::ops::Deref for Held<'_> {
    type Target = sqlx::PgConnection;
    fn deref(&self) -> &Self::Target {
        match self {
            Held::Borrowed(conn) => conn,
            Held::Owned(conn) => conn,
        }
    }
}

impl std::ops::DerefMut for Held<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Held::Borrowed(conn) => conn,
            Held::Owned(conn) => conn,
        }
    }
}

impl Db<'_> {
    pub(super) async fn conn(&mut self) -> crate::error::Result<Held<'_>> {
        Ok(match self {
            Db::Conn(conn) => Held::Borrowed(conn),
            Db::Pool(pool) => Held::Owned(pool.acquire().await?),
        })
    }
}

/// A master key version as the `master_key_version` column stores it.
pub(super) fn version_column(version: u32) -> crate::error::Result<i32> {
    i32::try_from(version).map_err(|_| {
        encryption::Error::ProviderRefused {
            provider: "?".into(),
            message: format!("master key version {version} is out of range"),
        }
        .into()
    })
}

/// The data key an active row holds, through the key service
/// (docs/architecture.md §230): the key that wrapped it, or the legacy
/// master key for a row from before providers. A key this process hasn't
/// used yet - one another instance gave a bounded context - is looked up
/// once among the keys skilj records, on `executor`, and tried again.
///
/// A row marked unreadable that unwraps after all - its key was put right -
/// has its mark cleared, so it counts again for the rewrap, the retirement
/// and the key's deletion, and when it is behind the bounded context's key
/// that key's rewrap is made pending again, to bring it there.
pub(super) async fn unwrap_row(
    db: &mut Db<'_>,
    bounded_context: &str,
    key_service: &KeyService,
    row: &EncryptionKeyRow,
) -> crate::error::Result<DataKey> {
    let wrapped = row
        .wrapped_key
        .as_deref()
        .expect("an active encryption_keys row always still has its wrapped key");
    let stored = encryption::StoredWrap {
        provider: row.key_provider.as_deref(),
        version: row.master_key_version.and_then(|v| u32::try_from(v).ok()),
        ciphertext: wrapped,
        nonce: row.wrap_nonce.as_deref(),
    };
    let data_key = match key_service.unwrap(stored).await {
        Err(encryption::Error::ProviderChanged { .. }) => {
            master_key::register_known_keys(&mut *db.conn().await?, key_service).await?;
            key_service.unwrap(stored).await?
        }
        other => other?,
    };
    if row.unreadable_since.is_some() {
        // Readable after all, and no rewrap is looking: the one that
        // marked it is done. The bounded context's key gets its rewrap
        // pending again, so the next check brings this data key to it -
        // and a key the context used before is not kept in use by it
        // (docs/architecture.md §247). Before the mark is cleared: a
        // rewrap batch takes the key's row and then the data key's, and
        // the other order here could deadlock with one.
        //
        // Both in one transaction - a savepoint in a caller's - so no
        // rewrap check finds the rewrap pending and the data key still
        // marked: it would pass the data key by and mark the rewrap done,
        // leaving the data key where it is for good.
        let its_key =
            master_key::bounded_context_key(&mut *db.conn().await?, bounded_context).await?;
        let current = key_service.provider_for(its_key.as_deref())?;
        let schema = schema_ident(bounded_context);
        let mut conn = db.conn().await?;
        let mut tx = sqlx::Connection::begin(&mut *conn).await?;
        master_key::mark_data_keys_behind_for(
            &mut *tx,
            current.name(),
            row.key_provider.as_deref(),
            row.master_key_version,
        )
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.encryption_keys SET unreadable_since = NULL \
             WHERE id = $1 AND wrapped_key = $2"
        )))
        .bind(row.id)
        .bind(wrapped)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(data_key)
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
/// spec requires. A no-op, no `key_service` needed, when `sensitive_fields`
/// is empty or names no subjects actually present in `payload` - the
/// identical "empty is a no-op" register `protect_sensitive_fields`
/// itself is in. `key_service: None` with something to resolve is a real,
/// actionable configuration error, not a silent skip.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn resolve_encryption_keys(
    pool: &Pool,
    bounded_context: &str,
    sensitive_fields: &[SensitiveField],
    payload: &str,
    key_service: Option<&KeyService>,
    resolved: &mut std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
) -> crate::error::Result<()> {
    let subjects = crate::event_store::try_sensitive_field_subjects(sensitive_fields, payload)?;
    if subjects.is_empty() {
        return Ok(());
    }
    let key_service = key_service.ok_or(encryption::Error::MasterKeyNotConfigured)?;
    for (subject_key, subject_value) in subjects {
        if resolved.contains_key(&(subject_key.clone(), subject_value.clone())) {
            continue;
        }
        let provisioned = get_or_create_encryption_key(
            pool,
            bounded_context,
            &subject_key,
            &subject_value,
            key_service,
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
/// so an unauthorised caller's query needs no `key_service` at all, no
/// matter how many sensitive fields the payload declares. `key_service:
/// None` with at least one *granted* subject to resolve is a real,
/// actionable configuration error (confirmed via `AskUserQuestion` before
/// building - mirrors `resolve_encryption_keys`'s own identical
/// precedent) - a caller who *is* entitled to see a decrypted value
/// deserves to know the server can't produce one, not silent ciphertext
/// indistinguishable from "you're not authorised." A subject that *is*
/// granted but has no active key (destroyed by `ForgetSubject`, or never
/// provisioned) is recorded in `resolved` as `None` - `render_event`/
/// `render_command`'s own `resolve_data_key` closure sees `None` for it,
/// the correct crypto-shredding outcome, not an error. Recording the
/// miss, rather than leaving the subject out, is what stops a forgotten
/// subject shared across a page of events from being looked up again
/// for every one of them.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn resolve_data_keys_for_reading(
    pool: &Pool,
    bounded_context: &str,
    sensitive_fields: &[SensitiveField],
    payload: &str,
    access_mapping: &crate::access_control::RoleAccessMapping,
    key_service: Option<&KeyService>,
    resolved: &mut std::collections::HashMap<(String, String), Option<DataKey>>,
) -> crate::error::Result<()> {
    let granted_subjects: Vec<(String, String)> =
        crate::event_store::try_sensitive_field_subjects(sensitive_fields, payload)?
            .into_iter()
            .filter(|(_, subject_value)| {
                crate::event_store::sensitive_field_is_granted(access_mapping, subject_value)
            })
            .collect();
    if granted_subjects.is_empty() {
        return Ok(());
    }
    let key_service = key_service.ok_or(encryption::Error::MasterKeyNotConfigured)?;
    for (subject_key, subject_value) in granted_subjects {
        if resolved.contains_key(&(subject_key.clone(), subject_value.clone())) {
            continue;
        }
        let data_key = get_active_data_key(
            pool,
            bounded_context,
            &subject_key,
            &subject_value,
            key_service,
        )
        .await?;
        resolved.insert((subject_key, subject_value), data_key);
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

pub(super) async fn get_active_encryption_key_row(
    executor: impl sqlx::PgExecutor<'_>,
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
    .fetch_optional(executor)
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
///
/// Refuses the secrets namespace (`secrets::is_reserved_subject_key`): a
/// secret version's key is only ever unwrapped by `fetch_secret_value`,
/// which goes through the private `get_active_data_key_on`. An
/// application's declared `SensitiveField` cannot read a secret's key.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_active_data_key(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    key_service: &KeyService,
) -> crate::error::Result<Option<DataKey>> {
    if secrets::is_reserved_subject_key(subject_key) {
        return Err(secrets::Error::ReservedSubjectKey.into());
    }
    get_active_data_key_on(
        pool,
        bounded_context,
        subject_key,
        subject_value,
        key_service,
    )
    .await
}

/// [`get_active_data_key`] without the reserved-namespace guard - the shared
/// body, used by `fetch_secret_value` (which is *how* the reserved
/// namespace is meant to be reached) and by the public wrapper above.
pub(super) async fn get_active_data_key_on(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    key_service: &KeyService,
) -> crate::error::Result<Option<DataKey>> {
    match get_active_encryption_key_row(pool, bounded_context, subject_key, subject_value).await? {
        Some(row) => Ok(Some(
            unwrap_row(&mut Db::Pool(pool), bounded_context, key_service, &row).await?,
        )),
        None => Ok(None),
    }
}

/// Resolve only the active data key for the declared projection subject.
/// An absent or forgotten key needs no configured key service.
pub async fn projection_subject_data_key(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    key_service: Option<&KeyService>,
) -> crate::error::Result<Option<DataKey>> {
    if secrets::is_reserved_subject_key(subject_key) {
        return Err(secrets::Error::ReservedSubjectKey.into());
    }
    let Some(row) =
        get_active_encryption_key_row(pool, bounded_context, subject_key, subject_value).await?
    else {
        return Ok(None);
    };
    let key_service = key_service.ok_or(encryption::Error::MasterKeyNotConfigured)?;
    unwrap_row(&mut Db::Pool(pool), bounded_context, key_service, &row)
        .await
        .map(Some)
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
/// nothing matches - no `key_service` needed at all in that case, the
/// identical "nothing to resolve, no key required" property
/// `resolve_data_keys_for_reading` already has. A **non-empty** match
/// with `key_service: None` is the identical hard, actionable
/// configuration error `resolve_encryption_keys`/`resolve_data_keys_for_reading`
/// already raise for the same underlying reason - confirmed with the
/// user before building real decrypt-on-read at all.
///
/// Excludes the secrets namespace (`secrets::SUBJECT_KEY`): a projection
/// never holds a secret version's ciphertext, and a secret's key is only
/// ever unwrapped by `fetch_secret_value`, so a subject value that
/// happens to match a secret version must not hand its `DataKey` out here.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_active_data_keys_for_subject_value(
    pool: &Pool,
    bounded_context: &str,
    subject_value: &str,
    key_service: Option<&KeyService>,
) -> crate::error::Result<Vec<DataKey>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<EncryptionKeyRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {ENCRYPTION_KEY_COLUMNS} FROM {schema}.encryption_keys \
         WHERE subject_value = $1 AND status = 'active' AND subject_key <> $2"
    )))
    .bind(subject_value)
    .bind(secrets::SUBJECT_KEY)
    .fetch_all(pool)
    .await?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let key_service = key_service.ok_or(encryption::Error::MasterKeyNotConfigured)?;
    let mut data_keys = Vec::with_capacity(rows.len());
    let mut db = Db::Pool(pool);
    for row in &rows {
        data_keys.push(unwrap_row(&mut db, bounded_context, key_service, row).await?);
    }
    Ok(data_keys)
}

/// Persists `forget_subject`'s outcome - real crypto-shredding, not just
/// the status flip: `wrapped_key`/`wrap_nonce` are set to `NULL` in the
/// same statement, so even the master key can no longer recover this
/// subject's `DataKey` afterwards (see `forget_subject`'s own doc comment:
/// "this function's own caller enacts \[it\] by destroying the key row").
/// Scoped to `status = 'active'` in the `WHERE` clause purely
/// defensively, since the pure rule already rejects an inactive key
/// before this is ever called.
///
/// Refuses the secrets namespace (`secrets::is_reserved_subject_key`): a
/// secret version's key is only ever shredded by `put_secret_value`/
/// `revoke_secret`, which retire the version row in the same transaction.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn destroy_encryption_key(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    destroyed_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    if secrets::is_reserved_subject_key(subject_key) {
        return Err(secrets::Error::ReservedSubjectKey.into());
    }
    let schema = schema_ident(bounded_context);
    destroy_encryption_key_on(pool, &schema, subject_key, subject_value, destroyed_at).await
}

/// [`destroy_encryption_key`] on any executor, with the schema already in
/// hand - so a secret rotation (`put_secret_value`) or revocation
/// (`revoke_secret`) can shred the superseded version's key in the *same*
/// transaction that retires it, never a second pooled connection and never
/// a window where the version is gone but its key is not.
pub(super) async fn destroy_encryption_key_on(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    subject_key: &str,
    subject_value: &str,
    destroyed_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.encryption_keys SET status = 'destroyed', destroyed_at = $1, \
         wrapped_key = NULL, wrap_nonce = NULL \
         WHERE subject_key = $2 AND subject_value = $3 AND status = 'active'"
    )))
    .bind(destroyed_at)
    .bind(subject_key)
    .bind(subject_value)
    .execute(executor)
    .await?;
    Ok(())
}
