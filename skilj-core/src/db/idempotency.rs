//! Persistence for idempotency rule groups.

use super::*;

/// A hit returns the stored `triggered_event_sequences` from a prior
/// `Accepted` outcome for this exact `(command_type_name, client_id,
/// idempotency_key)` triple - `submit_command`'s own short-circuit. Must
/// only be called after the bounded context's own `sequence` row lock
/// is already held (see `submit_command`'s own doc comment) - that lock
/// is what makes this plain, unlocked `SELECT` race-free, the same way
/// it already makes the DCB-conflict recheck a few lines below it
/// race-free.
///
/// Whether this `(command type, client, key)` already produced an
/// accepted command. Approval-required submissions check this *before*
/// creating a proposal: once an approver accepted a proposal submitted
/// with a caller key, a duplicate submission must short-circuit to the
/// ordinary dedup path rather than proposing the same command again.
pub async fn idempotency_key_already_used(
    pool: &Pool,
    bounded_context: &str,
    command_type_name: &str,
    client_id: &str,
    idempotency_key: &str,
) -> crate::error::Result<bool> {
    let schema = schema_ident(bounded_context);
    Ok(
        lookup_idempotency_key(pool, &schema, command_type_name, client_id, idempotency_key)
            .await?
            .is_some(),
    )
}

/// A real `client_id`-only match, never a `client_id = ''` legacy row -
/// see `migrate_idempotency_keys_client_id_scoping`'s own doc comment
/// for why a pre-migration row is deliberately left permanently
/// unmatchable rather than kept as a fallback: the user's own explicit
/// call, choosing to fully close the cross-tenant collision this whole
/// fix exists for over preserving those specific rows' dedup power.
pub(super) async fn lookup_idempotency_key<'e>(
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
pub(super) async fn insert_idempotency_key<'e>(
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
