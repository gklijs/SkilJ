//! The master keys' storage side (docs/architecture.md §230): one state
//! row per key - the default key and every bounded context's own - the
//! bounded contexts given keys of their own, the master-key trail's
//! events, startup's checks of the stored data keys, the rewrap, the
//! retirement and the deletion of a key no bounded context uses any more.

use super::{
    get_bounded_context, get_event_type, insert_event, next_sequence,
    require_admin_bounded_context, schema_ident, upsert_event_type, version_column, Pool,
};
use crate::encryption::provider::{KeyProvider, WrappedDataKey};
use crate::encryption::trail::{
    master_key_event_type, MasterKeyChange, MasterKeyEventKind, RotationReason,
};
use crate::encryption::{self, KeyService, StoredWrap};
use crate::event_store::BoundedContext;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction};
use std::collections::{BTreeMap, BTreeSet};

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

/// One master key and where its rotation stands: an
/// `encryption_master_keys` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterKeyState {
    /// The key's provider name.
    pub provider: String,
    /// The key's name for a bounded context's own key, `None` for the
    /// default key.
    pub key: Option<String>,
    /// The version new data keys are wrapped under and every data key is
    /// rewrapped to.
    pub version: u32,
    /// When it came into use.
    pub rotated_at: DateTime<Utc>,
    /// Whether the versions below retire as soon as the rewrap is done.
    pub retire_immediately: bool,
    /// When every data key of its bounded contexts was found wrapped under
    /// `version`; `None` while the rewrap is pending.
    pub rewrapped_at: Option<DateTime<Utc>>,
    /// Every version below this one is retired.
    pub retired_below: u32,
    /// Whether skilj created the key, and so deletes it when unused.
    pub created_by_skilj: bool,
    /// When the key, used by no bounded context, is deleted or let go.
    pub delete_after: Option<DateTime<Utc>>,
}

type StateRow = (
    String,
    Option<String>,
    i32,
    DateTime<Utc>,
    bool,
    Option<DateTime<Utc>>,
    i32,
    bool,
    Option<DateTime<Utc>>,
);

fn to_state(row: StateRow) -> MasterKeyState {
    let (
        provider,
        key,
        version,
        rotated_at,
        retire_immediately,
        rewrapped_at,
        retired_below,
        created_by_skilj,
        delete_after,
    ) = row;
    MasterKeyState {
        provider,
        key,
        version: u32::try_from(version).unwrap_or(0),
        rotated_at,
        retire_immediately,
        rewrapped_at,
        retired_below: u32::try_from(retired_below).unwrap_or(0),
        created_by_skilj,
        delete_after,
    }
}

const STATE_COLUMNS: &str = "provider, key, version, rotated_at, retire_immediately, \
     rewrapped_at, retired_below, created_by_skilj, delete_after";

/// Every master key's state row, the default key's first.
#[tracing::instrument(skip_all)]
pub async fn master_key_states(pool: &Pool) -> crate::error::Result<Vec<MasterKeyState>> {
    let rows: Vec<StateRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {STATE_COLUMNS} FROM encryption_master_keys ORDER BY key NULLS FIRST, provider"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(to_state).collect())
}

/// The state row of the key named `provider`, `None` before it was used.
#[tracing::instrument(skip_all)]
pub async fn master_key_state(
    pool: &Pool,
    provider: &str,
) -> crate::error::Result<Option<MasterKeyState>> {
    let row: Option<StateRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {STATE_COLUMNS} FROM encryption_master_keys WHERE provider = $1"
    )))
    .bind(provider)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(to_state))
}

async fn lock_key(tx: &mut Transaction<'_, Postgres>, provider: &str) -> crate::error::Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('skilj_encryption_master_key:' || $1))")
        .bind(provider)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn locked_state(
    tx: &mut Transaction<'_, Postgres>,
    provider: &str,
) -> crate::error::Result<Option<MasterKeyState>> {
    let row: Option<StateRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {STATE_COLUMNS} FROM encryption_master_keys WHERE provider = $1 FOR UPDATE"
    )))
    .bind(provider)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(to_state))
}

/// Makes every key skilj keeps state for known to `key_service`, so a data
/// key wrapped by any of them - a bounded context's previous key, or one
/// another instance assigned - finds its key.
pub async fn register_known_keys<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    key_service: &KeyService,
) -> crate::error::Result<()> {
    let keys: Vec<String> =
        sqlx::query_scalar("SELECT key FROM encryption_master_keys WHERE key IS NOT NULL")
            .fetch_all(executor)
            .await?;
    for key in keys {
        key_service.provider_for(Some(&key))?;
    }
    Ok(())
}

/// What [`rotate_master_key`] does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rotation {
    /// Records the provider's latest version as the one in use, without
    /// rotating: a key's first use (`Adopted`), or a rotation made outside
    /// skilj (`External`). Does nothing when it is already recorded.
    Record(RotationReason),
    /// Rotates at the provider, then records it.
    Rotate(RotationReason),
}

/// Records a new version of `provider`'s key in use, with
/// `EncryptionMasterKeyRotated`, in one transaction (docs/architecture.md
/// §230) - rotating at the provider first for [`Rotation::Rotate`]. `key`
/// is the key's name for a bounded context's own key. Under an advisory
/// lock per key held for the whole of it, so instances acting together
/// rotate once: `due`, when given, is checked under the lock. Answers the
/// version recorded, `None` when nothing was done.
///
/// A rotation that reached the provider but not the database is found the
/// next time: the provider's latest version is past the recorded one, and
/// it is recorded as `External`.
#[tracing::instrument(skip_all, fields(provider = %provider.name()))]
pub async fn rotate_master_key(
    pool: &Pool,
    provider: &dyn KeyProvider,
    key: Option<&str>,
    rotation: Rotation,
    retire_immediately: bool,
    actor_role_id: Option<&str>,
    due: Option<chrono::Duration>,
) -> crate::error::Result<Option<u32>> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    lock_key(&mut tx, provider.name()).await?;
    let current = locked_state(&mut tx, provider.name()).await?;
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
        "INSERT INTO encryption_master_keys \
         (provider, key, version, rotated_at, retire_immediately, rewrapped_at, retired_below) \
         VALUES ($1, $2, $3, $4, $5, NULL, $6) \
         ON CONFLICT (provider) DO UPDATE SET version = EXCLUDED.version, \
         rotated_at = EXCLUDED.rotated_at, retire_immediately = EXCLUDED.retire_immediately, \
         rewrapped_at = NULL",
    )
    .bind(provider.name())
    .bind(key)
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
            provider.name(),
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

/// Marks the rewrap of `provider`'s key to `version` done, when `version`
/// is still the one in use. Answers whether it did.
pub async fn mark_data_keys_rewrapped(
    pool: &Pool,
    provider: &str,
    version: u32,
) -> crate::error::Result<bool> {
    let updated = sqlx::query(
        "UPDATE encryption_master_keys SET rewrapped_at = $1 \
         WHERE provider = $2 AND version = $3 AND rewrapped_at IS NULL",
    )
    .bind(Utc::now())
    .bind(provider)
    .bind(version_column(version)?)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected() == 1)
}

/// Marks the rewrap of `provider`'s key pending again: a data key behind
/// it was found after it was marked done, or a bounded context was moved
/// onto it.
pub async fn mark_data_keys_behind(pool: &Pool, provider: &str) -> crate::error::Result<()> {
    sqlx::query("UPDATE encryption_master_keys SET rewrapped_at = NULL WHERE provider = $1")
        .bind(provider)
        .execute(pool)
        .await?;
    Ok(())
}

/// Retires every version of `provider`'s key below `version` at the
/// provider and records `EncryptionMasterKeyRetired`, under the key's lock,
/// so it can't interleave with a rotation. Does nothing when they already
/// are.
#[tracing::instrument(skip_all, fields(provider = %provider.name()))]
pub async fn retire_master_key_versions(
    pool: &Pool,
    key_service: &KeyService,
    provider: &dyn KeyProvider,
    version: u32,
) -> crate::error::Result<bool> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    lock_key(&mut tx, provider.name()).await?;
    let Some(state) = locked_state(&mut tx, provider.name()).await? else {
        return Ok(false);
    };
    if state.version != version || state.retired_below >= version {
        return Ok(false);
    }
    provider.retire_below(version).await?;
    sqlx::query("UPDATE encryption_master_keys SET retired_below = $1 WHERE provider = $2")
        .bind(version_column(version)?)
        .bind(provider.name())
        .execute(&mut *tx)
        .await?;
    record(
        &mut tx,
        &admin,
        MasterKeyChange::retired(provider.name(), version),
    )
    .await?;
    tx.commit().await?;
    key_service.clear_cache();
    Ok(true)
}

/// A bounded context's own key, by its name at the provider; `None` for
/// one that uses the default key.
pub async fn bounded_context_key<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT key FROM encryption_key_assignments WHERE bounded_context = $1")
            .bind(bounded_context)
            .fetch_optional(executor)
            .await?,
    )
}

/// Whether skilj keeps state for the key named `provider` - false once it
/// was deleted.
pub(super) async fn key_recorded<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    provider: &str,
) -> crate::error::Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM encryption_master_keys WHERE provider = $1)",
    )
    .bind(provider)
    .fetch_one(executor)
    .await?)
}

/// Every bounded context with a key of its own, by its key's name.
pub async fn encryption_key_assignments(
    pool: &Pool,
) -> crate::error::Result<BTreeMap<String, String>> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT bounded_context, key FROM encryption_key_assignments")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().collect())
}

/// Gives `bounded_context` the key `key` of the default key's provider, or
/// the default key back for `None` (docs/architecture.md §230), recorded as
/// `EncryptionKeyAssigned` with `actor_role_id`. With `create`, skilj
/// creates the key at the provider first and, once no bounded context uses
/// it any more, deletes it again; otherwise it must exist already, and is
/// only ever let go of. The bounded context's data keys move to the new
/// key in the background: its rewrap is marked pending. Answers the key's
/// state.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn assign_encryption_key(
    pool: &Pool,
    key_service: &KeyService,
    bounded_context: &str,
    key: Option<&str>,
    create: bool,
    actor_role_id: Option<&str>,
) -> crate::error::Result<MasterKeyState> {
    get_bounded_context(pool, bounded_context)
        .await?
        .ok_or_else(crate::error::Error::row_not_found)?;
    let provider = key_service.provider_for(key)?;
    // Only a key this call really created is skilj's to delete later: a
    // provider's `create` answers whether it did, so a key that existed
    // already - a customer's - is never claimed.
    let created = if create && key.is_some() {
        provider.create().await?
    } else {
        false
    };
    // Refuses a key that doesn't exist or can't be reached before anything
    // is recorded, and records its first use.
    rotate_master_key(
        pool,
        provider.as_ref(),
        key,
        Rotation::Record(RotationReason::Adopted),
        false,
        actor_role_id,
        None,
    )
    .await?;
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    let previous: Option<String> = sqlx::query_scalar(
        "SELECT provider FROM encryption_key_assignments WHERE bounded_context = $1 FOR UPDATE",
    )
    .bind(bounded_context)
    .fetch_optional(&mut *tx)
    .await?;
    let previous = previous.unwrap_or_else(|| key_service.provider_name().to_string());
    match key {
        Some(key) => {
            sqlx::query(
                "INSERT INTO encryption_key_assignments \
                 (bounded_context, key, provider, assigned_at) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (bounded_context) DO UPDATE SET key = EXCLUDED.key, \
                 provider = EXCLUDED.provider, assigned_at = EXCLUDED.assigned_at",
            )
            .bind(bounded_context)
            .bind(key)
            .bind(provider.name())
            .bind(Utc::now())
            .execute(&mut *tx)
            .await?;
        }
        None => {
            sqlx::query("DELETE FROM encryption_key_assignments WHERE bounded_context = $1")
                .bind(bounded_context)
                .execute(&mut *tx)
                .await?;
        }
    }
    sqlx::query(
        "UPDATE encryption_master_keys SET rewrapped_at = NULL, delete_after = NULL, \
         created_by_skilj = created_by_skilj OR $2 WHERE provider = $1",
    )
    .bind(provider.name())
    .bind(created)
    .execute(&mut *tx)
    .await?;
    record(
        &mut tx,
        &admin,
        MasterKeyChange::assigned(
            provider.name(),
            bounded_context,
            &previous,
            created,
            actor_role_id,
        ),
    )
    .await?;
    tx.commit().await?;
    master_key_state(pool, provider.name())
        .await?
        .ok_or_else(crate::error::Error::row_not_found)
}

/// Forgets the assignments of bounded contexts that no longer exist.
pub async fn forget_assignments_of_deleted_contexts(
    pool: &Pool,
    existing: &BTreeSet<String>,
) -> crate::error::Result<()> {
    for bounded_context in encryption_key_assignments(pool).await?.into_keys() {
        if !existing.contains(&bounded_context) {
            sqlx::query("DELETE FROM encryption_key_assignments WHERE bounded_context = $1")
                .bind(&bounded_context)
                .execute(pool)
                .await?;
        }
    }
    Ok(())
}

/// Sets when an unused bounded-context key is deleted, if not set yet.
pub async fn schedule_key_deletion(
    pool: &Pool,
    provider: &str,
    at: DateTime<Utc>,
) -> crate::error::Result<()> {
    sqlx::query(
        "UPDATE encryption_master_keys SET delete_after = $2 \
         WHERE provider = $1 AND key IS NOT NULL AND delete_after IS NULL",
    )
    .bind(provider)
    .bind(at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Deletes an unused bounded-context key once its deletion is due: at the
/// provider when skilj created it, and its state row either way, recorded
/// as `EncryptionKeyDeleted`. Under the key's lock, and only while no
/// bounded context is assigned it again and - checked again under that
/// lock - no data key is wrapped by it. Its state row is taken `FOR
/// UPDATE`, which waits for any provisioning or rewrap batch that took it
/// `FOR SHARE` to wrap with the key, so their rows are seen; one that comes
/// later finds no row and wraps nothing with it. Answers whether it did.
#[tracing::instrument(skip_all, fields(provider = %provider.name()))]
pub async fn delete_unused_key(
    pool: &Pool,
    key_service: &KeyService,
    provider: &dyn KeyProvider,
) -> crate::error::Result<bool> {
    let admin = require_admin_bounded_context(pool).await?;
    let mut tx = pool.begin().await?;
    lock_key(&mut tx, provider.name()).await?;
    let Some(state) = locked_state(&mut tx, provider.name()).await? else {
        return Ok(false);
    };
    let assigned: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM encryption_key_assignments WHERE provider = $1)",
    )
    .bind(provider.name())
    .fetch_one(&mut *tx)
    .await?;
    let due = state.delete_after.is_some_and(|at| at <= Utc::now());
    if state.key.is_none() || assigned || !due {
        return Ok(false);
    }
    if key_in_use_on(&mut tx, provider.name()).await? {
        sqlx::query("UPDATE encryption_master_keys SET delete_after = NULL WHERE provider = $1")
            .bind(provider.name())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(false);
    }
    if state.created_by_skilj {
        provider.delete().await?;
    }
    sqlx::query("DELETE FROM encryption_master_keys WHERE provider = $1")
        .bind(provider.name())
        .execute(&mut *tx)
        .await?;
    record(
        &mut tx,
        &admin,
        MasterKeyChange::deleted(provider.name(), state.created_by_skilj),
    )
    .await?;
    tx.commit().await?;
    key_service.clear_cache();
    Ok(true)
}

/// The distinct providers named on `bounded_context`'s active data keys,
/// and whether any is from before key providers: one index probe per
/// provider, however many data keys there are.
async fn stored_providers(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<(Vec<String>, bool)> {
    let schema = schema_ident(bounded_context);
    let providers: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "WITH RECURSIVE named(provider) AS ( \
             (SELECT key_provider FROM {schema}.encryption_keys \
              WHERE status = 'active' AND key_provider IS NOT NULL \
              AND unreadable_since IS NULL \
              ORDER BY key_provider LIMIT 1) \
             UNION ALL \
             SELECT (SELECT k.key_provider FROM {schema}.encryption_keys k \
                     WHERE k.status = 'active' AND k.key_provider > named.provider \
                     AND k.unreadable_since IS NULL \
                     ORDER BY k.key_provider LIMIT 1) \
             FROM named WHERE named.provider IS NOT NULL \
         ) SELECT provider FROM named WHERE provider IS NOT NULL"
    )))
    .fetch_all(pool)
    .await?;
    let legacy: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT EXISTS (SELECT 1 FROM {schema}.encryption_keys \
         WHERE status = 'active' AND key_provider IS NULL AND wrapped_key IS NOT NULL \
         AND unreadable_since IS NULL)"
    )))
    .fetch_one(pool)
    .await?;
    Ok((providers, legacy))
}

/// What startup found in one bounded context's active data keys.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DataKeyWraps {
    /// Some are wrapped by the legacy master key (and it unwraps them).
    pub legacy: bool,
}

/// Startup's check of one bounded context's active data keys
/// (docs/architecture.md §230): refused with
/// `encryption_key_provider_changed` when one was wrapped by a key
/// `key_service` doesn't know (another provider, or a key nothing records),
/// with `encryption_key_material_changed` when a key answers to the right
/// name but unwraps none of the newest data keys it is recorded on, with
/// `encryption_legacy_master_key_required` when legacy ones are stored and
/// no legacy master key is configured, and with
/// `encryption_legacy_master_key_changed` when it doesn't unwrap the newest
/// of them. Call [`register_known_keys`] first.
///
/// `settled` holds the keys already checked this startup - verified, or
/// with no verdict because the provider didn't answer - so each key costs
/// a few unwraps once per startup, not once per bounded context, and an
/// unreachable provider one timeout, not one per bounded context.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn check_data_key_wraps(
    pool: &Pool,
    bounded_context: &str,
    key_service: &KeyService,
    settled: &mut BTreeSet<String>,
) -> crate::error::Result<DataKeyWraps> {
    let (providers, legacy) = stored_providers(pool, bounded_context).await?;
    if let Some(stored) = providers
        .iter()
        .find(|p| key_service.provider_named(p).is_none())
    {
        return Err(encryption::Error::ProviderChanged {
            stored: stored.clone(),
            configured: key_service.provider_name().to_string(),
        }
        .into());
    }
    let schema = schema_ident(bounded_context);
    // The same name doesn't prove the same key: a key file recreated at the
    // same path, or another Vault cluster with the same key path, names its
    // key alike. Unwrapping a few of the newest data keys tells them apart
    // before anything is wrapped by, or marked unreadable under, the wrong
    // key. Only a refusal of the ciphertext counts against the key: a data
    // key under a retired version says nothing about the key's material
    // (the rewrap marks it), and a provider that can't answer is no verdict.
    let pending: Vec<&String> = providers.iter().filter(|p| !settled.contains(*p)).collect();
    for provider in pending {
        let newest: Vec<(Vec<u8>, i32)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT wrapped_key, master_key_version FROM {schema}.encryption_keys \
             WHERE status = 'active' AND key_provider = $1 AND wrapped_key IS NOT NULL \
             AND unreadable_since IS NULL ORDER BY id DESC LIMIT {KEY_PROBES}"
        )))
        .bind(provider)
        .fetch_all(pool)
        .await?;
        let mut refused = 0;
        let mut settle = false;
        for (wrapped, version) in &newest {
            let probe = key_service
                .unwrap(StoredWrap {
                    provider: Some(provider),
                    version: u32::try_from(*version).ok(),
                    ciphertext: wrapped,
                    nonce: None,
                })
                .await;
            match probe {
                Ok(_) => {
                    settle = true;
                    break;
                }
                Err(encryption::Error::DecryptFailed) => refused += 1,
                Err(encryption::Error::KeyVersionRetired { .. }) => {}
                Err(err) => {
                    tracing::warn!(
                        provider = %provider,
                        error = %err,
                        "could not check the key's data keys at startup"
                    );
                    settle = true;
                    break;
                }
            }
        }
        if !settle && refused > 0 {
            return Err(encryption::Error::KeyMaterialChanged {
                provider: provider.clone(),
                bounded_context: bounded_context.to_string(),
            }
            .into());
        }
        // Only retired ones, or none: nothing told; another context may.
        if settle {
            settled.insert(provider.clone());
        }
    }
    if !legacy {
        return Ok(DataKeyWraps { legacy: false });
    }
    // Several of the newest, not just one: a single corrupt row must not
    // read as the wrong key and keep every instance from starting.
    let newest: Vec<(Vec<u8>, Option<Vec<u8>>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT wrapped_key, wrap_nonce FROM {schema}.encryption_keys \
         WHERE status = 'active' AND key_provider IS NULL AND wrapped_key IS NOT NULL \
         AND unreadable_since IS NULL ORDER BY id DESC LIMIT {LEGACY_PROBES}"
    )))
    .fetch_all(pool)
    .await?;
    for (wrapped, nonce) in &newest {
        match key_service
            .unwrap(StoredWrap {
                provider: None,
                version: None,
                ciphertext: wrapped,
                nonce: nonce.as_deref(),
            })
            .await
        {
            Ok(_) => return Ok(DataKeyWraps { legacy: true }),
            Err(encryption::Error::DecryptFailed) => {}
            Err(err) => return Err(err.into()),
        }
    }
    Err(encryption::Error::LegacyMasterKeyChanged {
        bounded_context: bounded_context.to_string(),
    }
    .into())
}

/// How many of a bounded context's newest legacy data keys startup tries
/// the legacy master key on.
const LEGACY_PROBES: i64 = 5;

/// How many of a bounded context's newest data keys per key startup tries
/// to unwrap, to tell the key's material apart from another with its name.
const KEY_PROBES: i64 = 5;

/// The condition for a data key that still counts: not marked unreadable,
/// or marked before `$since` - the key's last rotation, so each rotation
/// tries a marked data key once more, in case the cause was a key since
/// put right.
fn readable_since(since: &str) -> String {
    format!("(unreadable_since IS NULL OR unreadable_since < {since})")
}

/// Whether any of `bounded_context`'s active data keys is not wrapped by
/// `provider` under `version` or later - leaving out the ones marked
/// unreadable since `since`, the key's last rotation, which this rotation
/// already tried.
pub async fn data_keys_behind(
    pool: &Pool,
    bounded_context: &str,
    provider: &str,
    version: u32,
    since: DateTime<Utc>,
) -> crate::error::Result<bool> {
    let schema = schema_ident(bounded_context);
    let counts = readable_since("$3");
    let behind: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT EXISTS (SELECT 1 FROM {schema}.encryption_keys \
             WHERE status = 'active' AND key_provider IS NULL AND wrapped_key IS NOT NULL \
             AND {counts}) \
         OR EXISTS (SELECT 1 FROM {schema}.encryption_keys \
             WHERE status = 'active' AND (key_provider < $1 OR key_provider > $1) \
             AND {counts}) \
         OR EXISTS (SELECT 1 FROM {schema}.encryption_keys \
             WHERE status = 'active' AND key_provider = $1 AND master_key_version < $2 \
             AND {counts})"
    )))
    .bind(provider)
    .bind(version_column(version)?)
    .bind(since)
    .fetch_one(pool)
    .await?;
    Ok(behind)
}

/// Whether any bounded context still has an active data key wrapped by
/// `provider` - which keeps it from being deleted. One marked unreadable
/// counts too: the cause may be put right, and deleting the key would make
/// that impossible. One deleted meanwhile is skipped.
pub async fn key_in_use(pool: &Pool, provider: &str) -> crate::error::Result<bool> {
    let mut tx = pool.begin().await?;
    let used = key_in_use_on(&mut tx, provider).await?;
    tx.rollback().await?;
    Ok(used)
}

async fn key_in_use_on(
    conn: &mut sqlx::PgConnection,
    provider: &str,
) -> crate::error::Result<bool> {
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM bounded_contexts")
        .fetch_all(&mut *conn)
        .await?;
    for name in names {
        let schema = schema_ident(&name);
        // A savepoint, so a schema dropped meanwhile doesn't abort a
        // caller's transaction.
        sqlx::query("SAVEPOINT key_in_use")
            .execute(&mut *conn)
            .await?;
        let used = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(format!(
            "SELECT EXISTS (SELECT 1 FROM {schema}.encryption_keys \
             WHERE status = 'active' AND key_provider = $1)"
        )))
        .bind(provider)
        .fetch_one(&mut *conn)
        .await
        .map_err(crate::error::Error::from);
        match used {
            Ok(true) => {
                sqlx::query("RELEASE SAVEPOINT key_in_use")
                    .execute(&mut *conn)
                    .await?;
                return Ok(true);
            }
            Ok(false) => {}
            Err(err) if super::is_missing_schema(&err) => {
                sqlx::query("ROLLBACK TO SAVEPOINT key_in_use")
                    .execute(&mut *conn)
                    .await?;
            }
            Err(err) => return Err(err),
        }
        sqlx::query("RELEASE SAVEPOINT key_in_use")
            .execute(&mut *conn)
            .await?;
    }
    Ok(false)
}

/// What [`rewrap_data_keys`] did in one bounded context.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RewrapOutcome {
    /// Active data keys now wrapped by the target key's version in use.
    pub rewrapped: u64,
    /// Active data keys found unreadable - no configured key unwraps them -
    /// and marked so (`unreadable_since`): left as they were, and from then
    /// on no longer counted as behind, so they hold up neither the end of
    /// the rewrap nor the retirement.
    pub unreadable: u64,
}

/// How many data keys one rewrap batch covers.
const REWRAP_BATCH: i64 = 200;

type RewrapRow = (i64, Vec<u8>, Option<Vec<u8>>, Option<String>, Option<i32>);

/// Whether `err` says this one data key can't be unwrapped by any
/// configured key - corrupt, under a retired version, or wrapped by a key
/// nothing records - rather than that the provider or the configuration
/// is failing, which a later run may get past.
fn definitively_unreadable(err: &encryption::Error) -> bool {
    matches!(
        err,
        encryption::Error::DecryptFailed
            | encryption::Error::KeyVersionRetired { .. }
            | encryption::Error::ProviderChanged { .. }
    )
}

/// Rewraps every active data key of `bounded_context` that isn't wrapped
/// by `target` under `version` or later (docs/architecture.md §230): one
/// `target` already wrapped through its own rewrap (Vault does it without
/// the data key leaving it), one another key wrapped - the bounded
/// context's previous key - by unwrapping it with that key and wrapping it
/// with `target`, and a legacy one with the legacy master key. The data key
/// stays the same, so no event, command or secret is rewritten.
///
/// In batches, each its own transaction; each row is updated only if it
/// still holds what was read, so a concurrent `forgetSubject` or another
/// instance's rewrap wins. Each batch takes `target`'s state row `FOR
/// SHARE` before it writes, so a deletion of the key waits for it, and a
/// batch after a deletion finds no row and stops instead of storing data
/// keys wrapped by a deleted key.
///
/// A data key that is definitively unreadable is marked `unreadable_since`
/// in its batch; a provider or configuration failure ends the run with the
/// error instead, for the next one to retry. A data key marked before
/// `since` - the key's last rotation - is tried once more, in case the
/// cause was a key since put right; one that still fails is marked again.
///
/// `EncryptionDataKeysRewrapped`, with this run's counts, is recorded in the
/// transaction of the batch that leaves nothing behind - or, when the rows
/// left were taken away meanwhile, on its own once the run finds none - so
/// a crash can't leave the work done and the event missing. Only when the
/// run changed something.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn rewrap_data_keys(
    pool: &Pool,
    bounded_context: &str,
    key_service: &KeyService,
    target: &dyn KeyProvider,
    version: u32,
    since: DateTime<Utc>,
) -> crate::error::Result<RewrapOutcome> {
    // Before any batch's transaction: a second pooled connection while one
    // is held is what §117 rules out.
    let admin = require_admin_bounded_context(pool).await?;
    let schema = schema_ident(bounded_context);
    let target_name = target.name().to_string();
    let behind_filter = format!(
        "status = 'active' AND wrapped_key IS NOT NULL AND {} \
         AND (key_provider IS NULL OR key_provider <> $1 OR master_key_version < $2)",
        readable_since("$3")
    );
    let event = |outcome: RewrapOutcome| {
        MasterKeyChange::rewrapped(
            &target_name,
            version,
            bounded_context,
            outcome.rewrapped,
            outcome.unreadable,
        )
    };
    let mut outcome = RewrapOutcome::default();
    let mut after = 0i64;
    loop {
        let rows: Vec<RewrapRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id, wrapped_key, wrap_nonce, key_provider, master_key_version \
             FROM {schema}.encryption_keys WHERE {behind_filter} AND id > $4 \
             ORDER BY id LIMIT $5"
        )))
        .bind(&target_name)
        .bind(version_column(version)?)
        .bind(since)
        .bind(after)
        .bind(REWRAP_BATCH)
        .fetch_all(pool)
        .await?;
        let Some(last) = rows.last() else {
            // The rows the last batch saw behind it were taken away
            // meanwhile (a `forgetSubject`), so no batch recorded the event.
            if outcome != RewrapOutcome::default() {
                let mut tx = pool.begin().await?;
                record(&mut tx, &admin, event(outcome)).await?;
                tx.commit().await?;
            }
            break;
        };
        after = last.0;

        let mut done: Vec<(i64, Vec<u8>, WrappedDataKey)> = Vec::with_capacity(rows.len());
        let mut unreadable: Vec<(i64, Vec<u8>)> = Vec::new();
        let mut same_key = Vec::new();
        for (id, wrapped, nonce, provider, row_version) in rows {
            if provider.as_deref() == Some(target_name.as_str()) {
                same_key.push((
                    id,
                    WrappedDataKey {
                        version: row_version.and_then(|v| u32::try_from(v).ok()).unwrap_or(0),
                        ciphertext: wrapped,
                    },
                ));
                continue;
            }
            let unwrapped = key_service
                .unwrap(StoredWrap {
                    provider: provider.as_deref(),
                    version: row_version.and_then(|v| u32::try_from(v).ok()),
                    ciphertext: &wrapped,
                    nonce: nonce.as_deref(),
                })
                .await;
            match unwrapped {
                Ok(data_key) => {
                    let rewrapped = target.wrap(data_key.bytes()).await?;
                    done.push((id, wrapped, rewrapped));
                }
                Err(err) if definitively_unreadable(&err) => {
                    tracing::warn!(encryption_key_id = id, error = %err, "data key unreadable, marked");
                    unreadable.push((id, wrapped));
                }
                Err(err) => return Err(err.into()),
            }
        }
        if !same_key.is_empty() {
            let inputs: Vec<WrappedDataKey> = same_key.iter().map(|(_, w)| w.clone()).collect();
            let results = target.rewrap(&inputs).await?;
            for ((id, old), result) in same_key.into_iter().zip(results) {
                match result {
                    Ok(rewrapped) => done.push((id, old.ciphertext, rewrapped)),
                    // A per-item refusal is about that one ciphertext.
                    Err(err)
                        if definitively_unreadable(&err)
                            || matches!(err, encryption::Error::ProviderRefused { .. }) =>
                    {
                        tracing::warn!(encryption_key_id = id, error = %err, "data key unreadable, marked");
                        unreadable.push((id, old.ciphertext));
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }

        let mut tx = pool.begin().await?;
        let target_kept: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM encryption_master_keys WHERE provider = $1 FOR SHARE)",
        )
        .bind(&target_name)
        .fetch_one(&mut *tx)
        .await?;
        if !target_kept {
            return Err(encryption::Error::ProviderRefused {
                provider: target_name.clone(),
                message: "the key was deleted while its data keys were being rewrapped".into(),
            }
            .into());
        }
        if !done.is_empty() {
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
                     master_key_version = u.version, unreadable_since = NULL \
                 FROM UNNEST($2::BIGINT[], $3::BYTEA[], $4::BYTEA[], $5::INTEGER[]) \
                     AS u(id, old, new, version) \
                 WHERE k.id = u.id AND k.status = 'active' AND k.wrapped_key = u.old"
            )))
            .bind(&target_name)
            .bind(&ids)
            .bind(&old)
            .bind(&new)
            .bind(&versions)
            .execute(&mut *tx)
            .await?;
            outcome.rewrapped += updated.rows_affected();
        }
        if !unreadable.is_empty() {
            let ids: Vec<i64> = unreadable.iter().map(|(id, _)| *id).collect();
            let old: Vec<Vec<u8>> = unreadable.iter().map(|(_, old)| old.clone()).collect();
            let marked = sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.encryption_keys k SET unreadable_since = $1 \
                 FROM UNNEST($2::BIGINT[], $3::BYTEA[]) AS u(id, old) \
                 WHERE k.id = u.id AND k.status = 'active' AND k.wrapped_key = u.old"
            )))
            .bind(Utc::now())
            .bind(&ids)
            .bind(&old)
            .execute(&mut *tx)
            .await?;
            outcome.unreadable += marked.rows_affected();
        }
        let more: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT EXISTS (SELECT 1 FROM {schema}.encryption_keys \
             WHERE {behind_filter} AND id > $4)"
        )))
        .bind(&target_name)
        .bind(version_column(version)?)
        .bind(since)
        .bind(after)
        .fetch_one(&mut *tx)
        .await?;
        if !more && outcome != RewrapOutcome::default() {
            record(&mut tx, &admin, event(outcome)).await?;
        }
        tx.commit().await?;
        if !more {
            break;
        }
    }
    Ok(outcome)
}
