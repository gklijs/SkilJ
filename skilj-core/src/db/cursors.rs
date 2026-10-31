//! Persistence for cursors rule groups.

use super::*;

// --- ReadCursor ---

#[derive(sqlx::FromRow)]
pub(super) struct ReadCursorRow {
    pub(super) ack_mode: String,
    pub(super) sequence: i64,
    pub(super) updated_at: DateTime<Utc>,
    pub(super) checked_out_at: Option<DateTime<Utc>>,
}

/// Unlike `access_tokens`, no index/bounded-context-resolution step is
/// needed here: every caller already holds the full `EventReadToken` (and
/// so its own `event_type.bounded_context.name`) by the time this is
/// called - token resolution (which does need the index) already
/// happened first, in `get_event_read_token` above. Generic over `impl
/// sqlx::PgExecutor` (the same generalisation `insert_event` itself
/// already has) so `GET /v1/events/consume?mode=manual` can call this
/// inside the same transaction `lock_read_cursor_for_consume`'s own
/// advisory lock spans (issue #25's investigation,
/// docs/architecture.md §53) - see that function's own doc comment for
/// why a plain call against `&Pool` here wouldn't actually be protected
/// by a lock taken on a different pooled connection.
#[tracing::instrument(skip_all)]
pub async fn get_read_cursor(
    executor: impl sqlx::PgExecutor<'_>,
    token: &EventReadToken,
) -> crate::error::Result<Option<ReadCursor>> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    let row: Option<ReadCursorRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT ack_mode, sequence, updated_at, checked_out_at \
         FROM {schema}.read_cursors WHERE token_id = $1"
    )))
    .bind(&token.id)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|r| ReadCursor {
        token: token.clone(),
        ack_mode: ack_mode_from_str(&r.ack_mode),
        sequence: r.sequence,
        updated_at: r.updated_at,
        checked_out_at: r.checked_out_at,
    }))
}

/// The primitive that actually closes the `manual_ack` claim race
/// (issue #25's investigation, docs/architecture.md §53) - not
/// a row lock on `read_cursors` (a `SELECT ... FOR UPDATE` protects
/// nothing for a token's very first-ever call, before any row exists to
/// lock - proven by a real concurrent test that failed exactly there
/// before this fix, not assumed). `pg_advisory_xact_lock`, the identical
/// primitive `migrate_idempotency_keys_client_id_scoping` already
/// establishes - deliberately blocking, not `pg_try_advisory_xact_lock`:
/// two callers racing the same token should serialize (one waits a few
/// milliseconds for the other's short transaction to commit), not have
/// one immediately give up. Held for the rest of the caller's own
/// transaction, released automatically on commit or rollback. Must be
/// called on the SAME transaction whose own later `get_read_cursor`/
/// `apply_cursor_update` calls it's meant to protect - a bare
/// `pg_advisory_lock` against a `&Pool` would make this meaningless (the
/// lock and the work it protects could each land on a different pooled
/// connection entirely), the identical failure mode that function's own
/// doc comment already warns about for the migration guard.
#[tracing::instrument(skip_all)]
pub async fn lock_read_cursor_for_consume(
    tx: &mut Transaction<'_, Postgres>,
    token: &EventReadToken,
) -> crate::error::Result<()> {
    let key = format!(
        "read_cursor:{}:{}",
        token.event_type.bounded_context.name, token.id
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(super) async fn upsert_read_cursor(
    executor: impl sqlx::PgExecutor<'_>,
    cursor: &ReadCursor,
) -> crate::error::Result<()> {
    let schema = schema_ident(&cursor.token.event_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.read_cursors (token_id, ack_mode, sequence, updated_at, checked_out_at) \
         VALUES ($1,$2,$3,$4,$5) \
         ON CONFLICT (token_id) DO UPDATE SET \
            ack_mode = EXCLUDED.ack_mode, sequence = EXCLUDED.sequence, \
            updated_at = EXCLUDED.updated_at, checked_out_at = EXCLUDED.checked_out_at"
    )))
    .bind(&cursor.token.id)
    .bind(ack_mode_to_str(cursor.ack_mode))
    .bind(cursor.sequence)
    .bind(cursor.updated_at)
    .bind(cursor.checked_out_at)
    .execute(executor)
    .await?;
    Ok(())
}

/// Persists whatever `event_store::consume_events` decided to do to a
/// token's `ReadCursor` - see `CursorUpdate`'s own doc comment for why
/// that decision and this persistence step are two separate functions
/// (the pure/no-I/O split every rule in this crate keeps). Generic over
/// `impl sqlx::PgExecutor` (the same generalisation `insert_event` itself
/// already has) so `GET /v1/events/consume?mode=manual` can call this
/// inside the same transaction `get_read_cursor_for_update`'s own row
/// lock spans (issue #25's investigation, docs/architecture.md
/// §53) - without that, the read and this write would each be free to
/// land on a different pooled connection, making the lock meaningless
/// (the identical failure mode `migrate_idempotency_keys_client_id_scoping`'s
/// own doc comment warns about for a bare `pg_advisory_lock`).
#[tracing::instrument(skip_all)]
pub async fn apply_cursor_update(
    executor: impl sqlx::PgExecutor<'_>,
    token: &EventReadToken,
    update: &CursorUpdate,
) -> crate::error::Result<()> {
    match update {
        CursorUpdate::Created(cursor) => upsert_read_cursor(executor, cursor).await,
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
            .execute(executor)
            .await?;
            Ok(())
        }
        // issue #25's investigation (docs/architecture.md §53) -
        // `sequence`/`updated_at` deliberately untouched: a claim alone
        // never moves the cursor's own position, only `AcknowledgeEvents`
        // does.
        CursorUpdate::Claimed { checked_out_at } => {
            let schema = schema_ident(&token.event_type.bounded_context.name);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.read_cursors SET checked_out_at = $1 WHERE token_id = $2"
            )))
            .bind(checked_out_at)
            .bind(&token.id)
            .execute(executor)
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
/// `checked_out_at` is cleared unconditionally alongside `sequence`/
/// `updated_at` - `rule AcknowledgeEvents`' own `ensures` block does the
/// same, unconditionally, regardless of whether a live claim was even
/// held (issue #25's investigation, docs/architecture.md §53).
#[tracing::instrument(skip_all)]
pub async fn record_acknowledgement(
    executor: impl sqlx::PgExecutor<'_>,
    token: &EventReadToken,
    sequence: i64,
    updated_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.read_cursors SET sequence = $1, updated_at = $2, checked_out_at = NULL \
         WHERE token_id = $3"
    )))
    .bind(sequence)
    .bind(updated_at)
    .bind(&token.id)
    .execute(executor)
    .await?;
    Ok(())
}
