//! Persistence for blob references (issue #132, docs/architecture.md
//! §239): the `blobs` and `blob_references` tables in each bounded
//! context's own schema, the built-in [`PostgresBlobStore`], uploading and
//! reading a blob, and the checks an append makes on the references in
//! its payloads. See [`crate::blob`] for the contract.

use super::*;
use crate::blob::{
    BlobField, BlobRef, BlobState, BlobStore, ByteStream, HashingCounter, StoredBlob,
};
use crate::encryption::provider::BoxFuture;
use futures_util::StreamExt;

/// The tables, created with a bounded context's schema and, for one
/// provisioned before them, at startup. The bytes the Postgres backend
/// keeps are in `blob_bytes`, apart from the metadata, so the metadata
/// reads the same whichever store holds them. All three go with the
/// schema when the bounded context is deleted.
pub(super) fn blob_tables_ddl(schema: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {schema}.blobs (
            id TEXT PRIMARY KEY,
            sha256 TEXT NOT NULL,
            size BIGINT NOT NULL CHECK (size >= 0),
            media_type TEXT NOT NULL,
            storage_key TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            referenced_at TIMESTAMPTZ,
            deleted_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS blobs_pending ON {schema}.blobs (created_at)
            WHERE state = 'pending';
        CREATE TABLE IF NOT EXISTS {schema}.blob_bytes (
            storage_key TEXT PRIMARY KEY,
            bytes BYTEA NOT NULL
        );
        CREATE TABLE IF NOT EXISTS {schema}.blob_references (
            blob_id TEXT NOT NULL REFERENCES {schema}.blobs (id),
            event_sequence BIGINT,
            command_id BIGINT,
            field TEXT NOT NULL,
            CHECK ((event_sequence IS NULL) <> (command_id IS NULL))
        );
        CREATE INDEX IF NOT EXISTS blob_references_by_blob
            ON {schema}.blob_references (blob_id)"
    )
}

/// The blob tables, for a bounded context provisioned before they existed.
/// Checked in the catalog first, so a startup that finds them takes no
/// lock (docs/architecture.md §158).
pub async fn ensure_blob_tables(pool: &Pool, bounded_context: &str) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let (present,): (bool,) = sqlx::query_as(
        "SELECT to_regclass($1) IS NOT NULL AND to_regclass($2) IS NOT NULL \
         AND to_regclass($3) IS NOT NULL",
    )
    .bind(format!("{schema}.blobs"))
    .bind(format!("{schema}.blob_bytes"))
    .bind(format!("{schema}.blob_references"))
    .fetch_one(pool)
    .await?;
    if !present {
        sqlx::raw_sql(sqlx::AssertSqlSafe(blob_tables_ddl(&schema)))
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// How many bytes [`PostgresBlobStore::get`] reads per round trip.
const READ_CHUNK_BYTES: i64 = 1024 * 1024;

/// The built-in [`BlobStore`]: the bytes in the bounded context's own
/// `blob_bytes` table, so they share its backups, its transactions'
/// durability and its deletion. An upload is held in memory while it is
/// hashed and stored - `max_bytes` bounds that - and a read streams it
/// back a megabyte at a time.
#[derive(Clone)]
pub struct PostgresBlobStore {
    pool: Pool,
}

impl PostgresBlobStore {
    /// A store on `pool`.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl BlobStore for PostgresBlobStore {
    fn put<'a>(
        &'a self,
        bounded_context: &'a str,
        id: &'a str,
        mut bytes: ByteStream<'a>,
        max_bytes: u64,
    ) -> BoxFuture<'a, crate::error::Result<StoredBlob>> {
        Box::pin(async move {
            let mut counter = HashingCounter::new(max_bytes);
            let mut buffer = Vec::new();
            while let Some(chunk) = bytes.next().await {
                let chunk = chunk.map_err(|e| crate::error::Error::BlobStore(e.to_string()))?;
                counter.update(&chunk)?;
                buffer.extend_from_slice(&chunk);
            }
            let (size, sha256) = counter.finish();
            let schema = schema_ident(bounded_context);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {schema}.blob_bytes (storage_key, bytes) VALUES ($1, $2)"
            )))
            .bind(id)
            .bind(buffer)
            .execute(&self.pool)
            .await?;
            Ok(StoredBlob::new(id.to_string(), size, sha256))
        })
    }

    fn get<'a>(
        &'a self,
        bounded_context: &'a str,
        storage_key: &'a str,
    ) -> BoxFuture<'a, crate::error::Result<ByteStream<'static>>> {
        Box::pin(async move {
            let schema = schema_ident(bounded_context);
            let length: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT octet_length(bytes)::bigint FROM {schema}.blob_bytes WHERE storage_key = $1"
            )))
            .bind(storage_key)
            .fetch_optional(&self.pool)
            .await?;
            let Some((length,)) = length else {
                return Err(crate::error::Error::BlobStore(format!(
                    "no bytes stored at {storage_key:?}"
                )));
            };
            let pool = self.pool.clone();
            let storage_key = storage_key.to_string();
            let stream = futures_util::stream::unfold(0_i64, move |offset| {
                let pool = pool.clone();
                let schema = schema.clone();
                let storage_key = storage_key.clone();
                async move {
                    if offset >= length {
                        return None;
                    }
                    // `substring` on bytea is 1-based.
                    let chunk: Result<(Vec<u8>,), sqlx::Error> =
                        sqlx::query_as(sqlx::AssertSqlSafe(format!(
                            "SELECT substring(bytes FROM $2::int FOR $3::int) FROM {schema}.blob_bytes \
                             WHERE storage_key = $1"
                        )))
                        .bind(&storage_key)
                        .bind(offset + 1)
                        .bind(READ_CHUNK_BYTES)
                        .fetch_one(&pool)
                        .await;
                    Some(match chunk {
                        Ok((chunk,)) => (Ok(chunk), offset + READ_CHUNK_BYTES),
                        Err(e) => (Err(std::io::Error::other(e)), length),
                    })
                }
            });
            Ok(stream.boxed())
        })
    }

    fn delete<'a>(
        &'a self,
        bounded_context: &'a str,
        storage_key: &'a str,
    ) -> BoxFuture<'a, crate::error::Result<()>> {
        Box::pin(async move {
            let schema = schema_ident(bounded_context);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {schema}.blob_bytes WHERE storage_key = $1"
            )))
            .bind(storage_key)
            .execute(&self.pool)
            .await?;
            Ok(())
        })
    }
}

/// A stored blob's metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRecord {
    /// The reference a payload stores for it.
    pub reference: BlobRef,
    /// Where it is in its life; `None` for a state a newer version wrote.
    pub state: Option<BlobState>,
    /// Where the store keeps the bytes.
    pub storage_key: String,
    /// When it was uploaded.
    pub created_at: DateTime<Utc>,
}

type BlobRow = (String, String, i64, String, String, String, DateTime<Utc>);

const BLOB_COLUMNS: &str = "id, sha256, size, media_type, storage_key, state, created_at";

fn blob_record(row: BlobRow) -> BlobRecord {
    let (id, sha256, size, media_type, storage_key, state, created_at) = row;
    BlobRecord {
        reference: BlobRef {
            id,
            sha256,
            size: u64::try_from(size).unwrap_or(0),
            media_type,
        },
        state: BlobState::parse(&state),
        storage_key,
        created_at,
    }
}

/// Stores `bytes` in `store` as a new, `pending` blob of `bounded_context`
/// and answers the reference a payload can then carry. Upload first,
/// append second: nothing references it yet, so a failure after the bytes
/// are stored leaves at most bytes no row names, which this removes again
/// where it can.
pub async fn upload_blob(
    pool: &Pool,
    store: &dyn BlobStore,
    bounded_context: &str,
    media_type: &str,
    bytes: ByteStream<'_>,
    max_bytes: u64,
    now: DateTime<Utc>,
) -> crate::error::Result<BlobRef> {
    let id = crate::shared::try_generate_token_id()?;
    let stored = store.put(bounded_context, &id, bytes, max_bytes).await?;
    let schema = schema_ident(bounded_context);
    let inserted = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.blobs (id, sha256, size, media_type, storage_key, state, created_at) \
         VALUES ($1, $2, $3, $4, $5, 'pending', $6)"
    )))
    .bind(&id)
    .bind(&stored.sha256)
    .bind(i64::try_from(stored.size).unwrap_or(i64::MAX))
    .bind(media_type)
    .bind(&stored.storage_key)
    .bind(now)
    .execute(pool)
    .await;
    if let Err(e) = inserted {
        if let Err(cleanup) = store.delete(bounded_context, &stored.storage_key).await {
            tracing::warn!(
                bounded_context = %bounded_context,
                error = %cleanup,
                "an uploaded blob's bytes could not be removed after its row failed to insert"
            );
        }
        return Err(e.into());
    }
    Ok(BlobRef {
        id,
        sha256: stored.sha256,
        size: stored.size,
        media_type: media_type.to_string(),
    })
}

/// A blob's metadata, if `bounded_context` has it.
pub async fn get_blob(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<BlobRecord>> {
    let schema = schema_ident(bounded_context);
    let row: Option<BlobRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {BLOB_COLUMNS} FROM {schema}.blobs WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(blob_record))
}

/// A blob's bytes, streamed from `store`, with the hash and size checked
/// as they pass: the stream ends in an error rather than serving bytes
/// that aren't the ones the reference names. `None` when `bounded_context`
/// has no such blob or it was deleted. Access checks are the caller's -
/// the REST route that will serve this (the issue's phase 4) allows it
/// only to a caller who can read a record referencing the blob with that
/// field visible.
pub async fn read_blob(
    pool: &Pool,
    store: &dyn BlobStore,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<(BlobRef, ByteStream<'static>)>> {
    let Some(record) = get_blob(pool, bounded_context, id).await? else {
        return Ok(None);
    };
    if record.state != Some(BlobState::Pending) && record.state != Some(BlobState::Referenced) {
        return Ok(None);
    }
    let inner = store.get(bounded_context, &record.storage_key).await?;
    let expected = record.reference.clone();
    let verified = futures_util::stream::unfold(
        (inner, Some(HashingCounter::new(expected.size)), expected),
        |(mut inner, counter, expected)| async move {
            let mut counter = counter?;
            match inner.next().await {
                Some(Ok(chunk)) => match counter.update(&chunk) {
                    Ok(()) => Some((Ok(chunk), (inner, Some(counter), expected))),
                    Err(e) => Some((Err(std::io::Error::other(e)), (inner, None, expected))),
                },
                Some(Err(e)) => Some((Err(e), (inner, None, expected))),
                None => {
                    let (size, sha256) = counter.finish();
                    if size == expected.size && sha256 == expected.sha256 {
                        None
                    } else {
                        Some((
                            Err(std::io::Error::other(format!(
                                "blob {} does not match its reference: {size} bytes, sha256 {sha256}",
                                expected.id
                            ))),
                            (inner, None, expected),
                        ))
                    }
                }
            }
        },
    );
    Ok(Some((record.reference, verified.boxed())))
}

/// One blob reference an append is about to store: in a command's payload
/// (`event_sequence: None`) or in the payload of the event at
/// `event_sequence`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BlobClaim {
    pub(super) event_sequence: Option<i64>,
    pub(super) field: String,
    pub(super) reference: BlobRef,
}

fn claims(
    fields: &[BlobField],
    payload: &str,
    event_sequence: Option<i64>,
    out: &mut Vec<BlobClaim>,
) -> crate::error::Result<()> {
    for (field, reference) in crate::blob::declared_blob_refs(fields, payload)? {
        out.push(BlobClaim {
            event_sequence,
            field,
            reference,
        });
    }
    Ok(())
}

/// The blob references in `event`'s stored payload, each in a field its
/// type declares (pure - see [`check_blob_claims`] for the rest).
pub(super) fn event_blob_claims(
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    event: &Event,
) -> crate::error::Result<Vec<BlobClaim>> {
    let mut out = Vec::new();
    claims(
        &dispatcher.event_blob_fields(&event.bounded_context.name, &event.event_type.name),
        &event.payload,
        Some(event.sequence),
        &mut out,
    )?;
    Ok(out)
}

/// [`event_blob_claims`] for a command and every event it produced.
pub(super) fn command_blob_claims(
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    command: &Command,
    events: &[Event],
) -> crate::error::Result<Vec<BlobClaim>> {
    let mut out = Vec::new();
    claims(
        &dispatcher.command_blob_fields(&command.bounded_context.name, &command.command_type.name),
        &command.payload,
        None,
        &mut out,
    )?;
    for event in events {
        out.extend(event_blob_claims(dispatcher, event)?);
    }
    Ok(out)
}

/// Refuses an append whose `claims` name a blob `bounded_context` doesn't
/// have, has deleted, or holds with another size, hash or media type. On
/// the append's own connection, under its write lock: the rows are locked
/// `FOR SHARE` until it commits, so nothing deletes a blob between this
/// check and the reference being stored. No query at all when there are
/// no claims - the common case.
pub(super) async fn check_blob_claims(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
    claims: &[BlobClaim],
) -> crate::error::Result<()> {
    if claims.is_empty() {
        return Ok(());
    }
    let schema = schema_ident(bounded_context);
    let ids: Vec<&str> = claims.iter().map(|c| c.reference.id.as_str()).collect();
    let rows: Vec<BlobRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {BLOB_COLUMNS} FROM {schema}.blobs WHERE id = ANY($1) FOR SHARE"
    )))
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await?;
    let stored: std::collections::HashMap<String, BlobRecord> = rows
        .into_iter()
        .map(blob_record)
        .map(|record| (record.reference.id.clone(), record))
        .collect();
    for claim in claims {
        let refuse = |reason: &str| {
            Err(crate::event_store::Error::BlobReferenceInvalid {
                field: claim.field.clone(),
                reason: reason.to_string(),
            }
            .into())
        };
        let Some(record) = stored.get(&claim.reference.id) else {
            return refuse("this bounded context has no such blob");
        };
        if !matches!(
            record.state,
            Some(BlobState::Pending) | Some(BlobState::Referenced)
        ) {
            return refuse("the blob was deleted");
        }
        if record.reference != claim.reference {
            return refuse("its size, sha256 or media type differs from the stored blob's");
        }
    }
    Ok(())
}

/// Records `claims` once the append has written its rows -
/// `command_id` is the command row's id, for a claim in a command's own
/// payload - and marks each blob `referenced`. One round trip.
pub(super) async fn record_blob_claims(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
    claims: &[BlobClaim],
    command_id: Option<i64>,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    record_blob_claims_by_command(
        conn,
        bounded_context,
        &claims
            .iter()
            .map(|claim| (claim, command_id))
            .collect::<Vec<_>>(),
        now,
    )
    .await
}

/// [`record_blob_claims`] for claims of several commands at once (a batch
/// written as a set), each with its own command row id.
pub(super) async fn record_blob_claims_by_command(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
    claims: &[(&BlobClaim, Option<i64>)],
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    if claims.is_empty() {
        return Ok(());
    }
    let schema = schema_ident(bounded_context);
    let blob_ids: Vec<&str> = claims
        .iter()
        .map(|(c, _)| c.reference.id.as_str())
        .collect();
    let sequences: Vec<Option<i64>> = claims.iter().map(|(c, _)| c.event_sequence).collect();
    let command_ids: Vec<Option<i64>> = claims
        .iter()
        .map(|(c, id)| {
            if c.event_sequence.is_some() {
                None
            } else {
                *id
            }
        })
        .collect();
    let fields: Vec<&str> = claims.iter().map(|(c, _)| c.field.as_str()).collect();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "WITH recorded AS ( \
             INSERT INTO {schema}.blob_references (blob_id, event_sequence, command_id, field) \
             SELECT * FROM unnest($1::text[], $2::bigint[], $3::bigint[], $4::text[]) \
         ) \
         UPDATE {schema}.blobs SET state = 'referenced', referenced_at = $5 \
         WHERE id = ANY($1) AND state = 'pending'"
    )))
    .bind(&blob_ids)
    .bind(&sequences)
    .bind(&command_ids)
    .bind(&fields)
    .bind(now)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Where a blob is referenced: `(event_sequence, command_id, field)`, one
/// row per reference.
pub async fn blob_references(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Vec<(Option<i64>, Option<i64>, String)>> {
    let schema = schema_ident(bounded_context);
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_sequence, command_id, field FROM {schema}.blob_references \
         WHERE blob_id = $1 ORDER BY event_sequence NULLS FIRST, command_id"
    )))
    .bind(id)
    .fetch_all(pool)
    .await?)
}
