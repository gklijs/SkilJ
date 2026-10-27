//! The master key's storage side (docs/architecture.md §230): the
//! rotation state row, the master-key trail's events, startup's checks of
//! the stored data keys, the rewrap and the retirement check.

use super::{
    get_event_type, insert_event, list_bounded_contexts, next_sequence,
    require_admin_bounded_context, schema_ident, upsert_event_type, version_column, Pool,
};
use crate::encryption::provider::WrappedDataKey;
use crate::encryption::trail::{
    master_key_event_type, MasterKeyChange, MasterKeyEventKind, RotationReason,
};
use crate::encryption::{self, KeyService, StoredWrap};
use crate::event_store::BoundedContext;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction};

/// Registers the master-key trail's event types in the admin bounded
/// context, the way `register_access_event_types` registers the access
/// trail's: a missing one is added, a stale schema refreshed.
#[tracing::instrument(skip_all)]
pub async fn register_master_key_event_types(pool: &Pool) -> crate::error::Result<()> {
    let admin = require_admin_bounded_context(pool).await?;
    for kind in MasterKeyEventKind::ALL {
        let wanted = master_key_event_type(&admin, kind);
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

async fn record(
    tx: &mut Transaction<'_, Postgres>,
    admin: &BoundedContext,
    change: MasterKeyChange,
) -> crate::error::Result<()> {
    let sequence = next_sequence(&mut **tx, &admin.name).await?;
    let event = change.into_event(admin, sequence, Utc::now());
    insert_event(&mut **tx, &event, None).await
}

/// The master key version in use and where its rotation stands: the
/// `encryption_master_key` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterKeyState {
    /// The key provider's name.
    pub provider: String,
    /// The version new data keys are wrapped under and every data key is
    /// rewrapped to.
    pub version: u32,
    /// When it came into use.
    pub rotated_at: DateTime<Utc>,
    /// Whether the versions below retire as soon as the rewrap is done.
    pub retire_immediately: bool,
    /// When every data key was found wrapped under `version`; `None` while
    /// the rewrap is pending.
    pub rewrapped_at: Option<DateTime<Utc>>,
    /// Every version below this one is retired.
    pub retired_below: u32,
}

type StateRow = (String, i32, DateTime<Utc>, bool, Option<DateTime<Utc>>, i32);

fn to_state(row: StateRow) -> MasterKeyState {
    let (provider, version, rotated_at, retire_immediately, rewrapped_at, retired_below) = row;
    MasterKeyState {
        provider,
        version: u32::try_from(version).unwrap_or(0),
        rotated_at,
        retire_immediately,
        rewrapped_at,
        retired_below: u32::try_from(retired_below).unwrap_or(0),
    }
}

const STATE_COLUMNS: &str =
    "provider, version, rotated_at, retire_immediately, rewrapped_at, retired_below";

/// The state row, `None` before any process with a key provider started.
#[tracing::instrument(skip_all)]
pub async fn master_key_state(pool: &Pool) -> crate::error::Result<Option<MasterKeyState>> {
    let row: Option<StateRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {STATE_COLUMNS} FROM encryption_master_key WHERE id = 1"
    )))
    .fetch_optional(pool)
    .await?;
    Ok(row.map(to_state))
}

/// What [`rotate_master_key`] does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rotation {
    /// Records the provider's latest version as the one in use, without
    /// rotating: the first start (`Adopted`), or a rotation made outside
    /// skilj (`External`). Does nothing when it is already recorded.
    Record(RotationReason),
    /// Rotates at the provider, then records it.
    Rotate(RotationReason),
}

/// Records a new version in use, with `EncryptionMasterKeyRotated`, in one
/// transaction (docs/architecture.md §230) - rotating at the provider
/// first for [`Rotation::Rotate`]. Under an advisory lock held for the
/// whole of it, so instances acting together rotate once: `due`, when
/// given, is checked under the lock, and a rotation another instance just
/// made is found there. Answers the version recorded, `None` when nothing
/// was done.
///
/// A rotation that reached the provider but not the database is found the
/// next time: the provider's latest version is past the recorded one, and
/// it is recorded as `External`.
#[tracing::instrument(skip_all)]
pub async fn rotate_master_key(
    pool: &Pool,
    key_service: &KeyService,
    rotation: Rotation,
    retire_immediately: bool,
    actor_role_id: Option<&str>,
    due: Option<chrono::Duration>,
) -> crate::error::Result<Option<u32>> {
    let admin = require_admin_bounded_context(pool).await?;
    let provider = key_service.provider();
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('skilj_encryption_master_key'))")
        .execute(&mut *tx)
        .await?;
    let current: Option<StateRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {STATE_COLUMNS} FROM encryption_master_key WHERE id = 1 FOR UPDATE"
    )))
    .fetch_optional(&mut *tx)
    .await?;
    // A row naming another provider is a deployment that switched
    // providers with no data key wrapped by the old one (startup refuses
    // otherwise): this provider's history starts over.
    let current = current
        .map(to_state)
        .filter(|state| state.provider == key_service.provider_name());
    let now = Utc::now();
    if let (Some(every), Some(state)) = (due, &current) {
        if state.rewrapped_at.is_none() || state.rotated_at + every > now {
            return Ok(None);
        }
    }
    let (version, reason) = match rotation {
        Rotation::Record(reason) => {
            let latest = provider.versions().await?.latest;
            if current.as_ref().is_some_and(|s| s.version >= latest) {
                return Ok(None);
            }
            (latest, reason)
        }
        Rotation::Rotate(reason) => (provider.rotate().await?, reason),
    };
    let retired_below = match &current {
        Some(state) => state.retired_below,
        None => provider.versions().await?.min_decryption,
    };
    sqlx::query(
        "INSERT INTO encryption_master_key \
         (id, provider, version, rotated_at, retire_immediately, rewrapped_at, retired_below) \
         VALUES (1, $1, $2, $3, $4, NULL, $5) \
         ON CONFLICT (id) DO UPDATE SET provider = EXCLUDED.provider, \
         version = EXCLUDED.version, rotated_at = EXCLUDED.rotated_at, \
         retire_immediately = EXCLUDED.retire_immediately, rewrapped_at = NULL",
    )
    .bind(key_service.provider_name())
    .bind(version_column(version)?)
    .bind(now)
    .bind(retire_immediately)
    .bind(version_column(retired_below)?)
    .execute(&mut *tx)
    .await?;
    record(
        &mut tx,
        &admin,
        MasterKeyChange::rotated(
            key_service.provider_name(),
            version,
            current.as_ref().map(|s| s.version),
            reason,
            retire_immediately,
            actor_role_id,
        ),
    )
    .await?;
    tx.commit().await?;
    Ok(Some(version))
}

/// Marks the rewrap to `version` done, when `version` is still the one in
/// use. Answers whether it did.
pub async fn mark_data_keys_rewrapped(pool: &Pool, version: u32) -> crate::error::Result<bool> {
    let updated = sqlx::query(
        "UPDATE encryption_master_key SET rewrapped_at = $1 \
         WHERE id = 1 AND version = $2 AND rewrapped_at IS NULL",
    )
    .bind(Utc::now())
    .bind(version_column(version)?)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected() == 1)
}

/// Marks the rewrap pending again: a data key below the version in use was
/// found after it was marked done.
pub async fn mark_data_keys_behind(pool: &Pool, version: u32) -> crate::error::Result<()> {
    sqlx::query(
        "UPDATE encryption_master_key SET rewrapped_at = NULL WHERE id = 1 AND version = $1",
    )
    .bind(version_column(version)?)
    .execute(pool)
    .await?;
    Ok(())
}

/// Retires every version below `version` at the provider and records
/// `EncryptionMasterKeyRetired`, under the rotation lock, so it can't
/// interleave with a rotation. Does nothing when they already are.
#[tracing::instrument(skip_all)]
pub async fn retire_master_key_versions(
    pool: &Pool,
    key_service: &KeyService,
    version: u32,
) -> crate::error::Result<bool> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('skilj_encryption_master_key'))")
        .execute(&mut *tx)
        .await?;
    let current: Option<StateRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {STATE_COLUMNS} FROM encryption_master_key WHERE id = 1 FOR UPDATE"
    )))
    .fetch_optional(&mut *tx)
    .await?;
    let Some(state) = current.map(to_state) else {
        return Ok(false);
    };
    if state.version != version || state.retired_below >= version {
        return Ok(false);
    }
    key_service.provider().retire_below(version).await?;
    sqlx::query("UPDATE encryption_master_key SET retired_below = $1 WHERE id = 1")
        .bind(version_column(version)?)
        .execute(&mut *tx)
        .await?;
    record(
        &mut tx,
        &admin,
        MasterKeyChange::retired(key_service.provider_name(), version),
    )
    .await?;
    tx.commit().await?;
    key_service.clear_cache();
    Ok(true)
}

/// What startup found in one bounded context's active data keys.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DataKeyWraps {
    /// Some are wrapped by the legacy master key (and it unwraps them).
    pub legacy: bool,
}

/// Startup's check of one bounded context's active data keys
/// (docs/architecture.md §230): refused with
/// `encryption_key_provider_changed` when one was wrapped by another
/// provider or key, with `encryption_legacy_master_key_required` when
/// legacy ones are stored and no legacy master key is configured, and with
/// `encryption_legacy_master_key_changed` when it doesn't unwrap the newest
/// of them. A few index probes, however many data keys there are.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn check_data_key_wraps(
    pool: &Pool,
    bounded_context: &str,
    key_service: &KeyService,
) -> crate::error::Result<DataKeyWraps> {
    let schema = schema_ident(bounded_context);
    let other: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT key_provider FROM {schema}.encryption_keys WHERE status = 'active' \
         AND (key_provider < $1 OR key_provider > $1) LIMIT 1"
    )))
    .bind(key_service.provider_name())
    .fetch_optional(pool)
    .await?;
    if let Some(stored) = other {
        return Err(encryption::Error::ProviderChanged {
            stored,
            configured: key_service.provider_name().to_string(),
        }
        .into());
    }
    let newest_legacy: Option<(Vec<u8>, Option<Vec<u8>>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT wrapped_key, wrap_nonce FROM {schema}.encryption_keys \
             WHERE status = 'active' AND key_provider IS NULL AND wrapped_key IS NOT NULL \
             ORDER BY id DESC LIMIT 1"
        )))
        .fetch_optional(pool)
        .await?;
    let Some((wrapped, nonce)) = newest_legacy else {
        return Ok(DataKeyWraps { legacy: false });
    };
    match key_service
        .unwrap(StoredWrap {
            provider: None,
            version: None,
            ciphertext: &wrapped,
            nonce: nonce.as_deref(),
        })
        .await
    {
        Ok(_) => Ok(DataKeyWraps { legacy: true }),
        Err(encryption::Error::DecryptFailed) => {
            Err(encryption::Error::LegacyMasterKeyChanged.into())
        }
        Err(err) => Err(err.into()),
    }
}

/// Whether any of `bounded_context`'s active data keys is not wrapped by
/// `key_service`'s provider under `version` or later.
pub async fn data_keys_behind(
    pool: &Pool,
    bounded_context: &str,
    key_service: &KeyService,
    version: u32,
) -> crate::error::Result<bool> {
    let schema = schema_ident(bounded_context);
    let behind: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT EXISTS (SELECT 1 FROM {schema}.encryption_keys \
             WHERE status = 'active' AND key_provider IS NULL AND wrapped_key IS NOT NULL) \
         OR EXISTS (SELECT 1 FROM {schema}.encryption_keys \
             WHERE status = 'active' AND (key_provider < $1 OR key_provider > $1)) \
         OR EXISTS (SELECT 1 FROM {schema}.encryption_keys \
             WHERE status = 'active' AND key_provider = $1 AND master_key_version < $2)"
    )))
    .bind(key_service.provider_name())
    .bind(version_column(version)?)
    .fetch_one(pool)
    .await?;
    Ok(behind)
}

/// What [`rewrap_data_keys`] did in one bounded context.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RewrapOutcome {
    /// Active data keys now wrapped under the version in use.
    pub rewrapped: u64,
    /// Active data keys that could not be unwrapped, left as they were.
    pub unreadable: u64,
}

/// How many data keys one rewrap batch covers.
const REWRAP_BATCH: i64 = 200;

type RewrapRow = (i64, Vec<u8>, Option<Vec<u8>>, Option<String>, Option<i32>);

/// Rewraps every active data key of `bounded_context` that isn't wrapped
/// by `key_service`'s provider under `version` or later
/// (docs/architecture.md §230): one the provider wrapped through its own
/// rewrap (Vault does it without the data key leaving it), a legacy one by
/// unwrapping it with the legacy master key and wrapping it with the
/// provider. The data key stays the same, so no event, command or secret
/// is rewritten. In batches; each row is updated only if it still holds
/// what was read, so a concurrent `forgetSubject` or another instance's
/// rewrap wins. Records `EncryptionDataKeysRewrapped` when it changed
/// anything.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn rewrap_data_keys(
    pool: &Pool,
    bounded_context: &str,
    key_service: &KeyService,
    version: u32,
) -> crate::error::Result<RewrapOutcome> {
    let schema = schema_ident(bounded_context);
    let provider_name = key_service.provider_name().to_string();
    let mut outcome = RewrapOutcome::default();
    let mut after = 0i64;
    loop {
        let rows: Vec<RewrapRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id, wrapped_key, wrap_nonce, key_provider, master_key_version \
             FROM {schema}.encryption_keys \
             WHERE status = 'active' AND wrapped_key IS NOT NULL AND id > $1 \
             AND (key_provider IS NULL OR key_provider <> $2 OR master_key_version < $3) \
             ORDER BY id LIMIT $4"
        )))
        .bind(after)
        .bind(&provider_name)
        .bind(version_column(version)?)
        .bind(REWRAP_BATCH)
        .fetch_all(pool)
        .await?;
        let Some(last) = rows.last() else {
            break;
        };
        after = last.0;

        let mut done: Vec<(i64, Vec<u8>, WrappedDataKey)> = Vec::with_capacity(rows.len());
        let mut by_provider = Vec::new();
        for (id, wrapped, nonce, provider, row_version) in rows {
            match (provider.as_deref(), row_version) {
                (Some(p), Some(v)) if p == provider_name => {
                    by_provider.push((
                        id,
                        WrappedDataKey {
                            version: u32::try_from(v).unwrap_or(0),
                            ciphertext: wrapped,
                        },
                    ));
                }
                (Some(_), _) => outcome.unreadable += 1,
                (None, _) => {
                    let unwrapped = key_service
                        .unwrap(StoredWrap {
                            provider: None,
                            version: None,
                            ciphertext: &wrapped,
                            nonce: nonce.as_deref(),
                        })
                        .await;
                    match unwrapped {
                        Ok(data_key) => {
                            let rewrapped = key_service.provider().wrap(data_key.bytes()).await?;
                            done.push((id, wrapped, rewrapped));
                        }
                        Err(err) => {
                            tracing::warn!(encryption_key_id = id, error = %err, "legacy data key not rewrapped");
                            outcome.unreadable += 1;
                        }
                    }
                }
            }
        }
        if !by_provider.is_empty() {
            let inputs: Vec<WrappedDataKey> = by_provider.iter().map(|(_, w)| w.clone()).collect();
            let results = key_service.provider().rewrap(&inputs).await?;
            for ((id, old), result) in by_provider.into_iter().zip(results) {
                match result {
                    Ok(rewrapped) => done.push((id, old.ciphertext, rewrapped)),
                    Err(err) => {
                        tracing::warn!(encryption_key_id = id, error = %err, "data key not rewrapped");
                        outcome.unreadable += 1;
                    }
                }
            }
        }
        if done.is_empty() {
            continue;
        }
        let ids: Vec<i64> = done.iter().map(|(id, _, _)| *id).collect();
        let old: Vec<Vec<u8>> = done.iter().map(|(_, old, _)| old.clone()).collect();
        let new: Vec<Vec<u8>> = done.iter().map(|(_, _, w)| w.ciphertext.clone()).collect();
        let versions: Vec<i32> = done
            .iter()
            .map(|(_, _, w)| version_column(w.version))
            .collect::<crate::error::Result<_>>()?;
        let updated = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.encryption_keys k \
             SET wrapped_key = u.new, wrap_nonce = NULL, key_provider = $1, \
                 master_key_version = u.version \
             FROM UNNEST($2::BIGINT[], $3::BYTEA[], $4::BYTEA[], $5::INTEGER[]) \
                 AS u(id, old, new, version) \
             WHERE k.id = u.id AND k.status = 'active' AND k.wrapped_key = u.old"
        )))
        .bind(&provider_name)
        .bind(&ids)
        .bind(&old)
        .bind(&new)
        .bind(&versions)
        .execute(pool)
        .await?;
        outcome.rewrapped += updated.rows_affected();
    }
    if outcome != RewrapOutcome::default() {
        let admin = require_admin_bounded_context(pool).await?;
        let mut tx = pool.begin().await?;
        record(
            &mut tx,
            &admin,
            MasterKeyChange::rewrapped(
                &provider_name,
                version,
                bounded_context,
                outcome.rewrapped,
                outcome.unreadable,
            ),
        )
        .await?;
        tx.commit().await?;
    }
    Ok(outcome)
}

/// The bounded contexts with a data key behind `version`, every one of
/// them - for the rewrap and the retirement check. One deleted meanwhile
/// is skipped.
pub async fn bounded_contexts_behind(
    pool: &Pool,
    key_service: &KeyService,
    version: u32,
) -> crate::error::Result<Vec<String>> {
    let mut behind = Vec::new();
    for bounded_context in list_bounded_contexts(pool).await? {
        match data_keys_behind(pool, &bounded_context.name, key_service, version).await {
            Ok(true) => behind.push(bounded_context.name),
            Ok(false) => {}
            Err(err) if super::is_missing_schema(&err) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(behind)
}
