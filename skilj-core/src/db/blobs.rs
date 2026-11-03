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
            subject_key TEXT,
            subject_value TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            grace_from TIMESTAMPTZ NOT NULL,
            referenced_at TIMESTAMPTZ,
            deleted_at TIMESTAMPTZ,
            bytes_removed_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS blobs_pending ON {schema}.blobs (grace_from)
            WHERE state = 'pending';
        CREATE INDEX IF NOT EXISTS blobs_to_remove ON {schema}.blobs (id)
            WHERE state = 'deleted' AND bytes_removed_at IS NULL;
        CREATE INDEX IF NOT EXISTS blobs_by_subject ON {schema}.blobs (subject_key, subject_value)
            WHERE subject_key IS NOT NULL;
        CREATE TABLE IF NOT EXISTS {schema}.blob_pins (
            blob_id TEXT NOT NULL REFERENCES {schema}.blobs (id),
            holder_kind TEXT NOT NULL,
            holder_id TEXT NOT NULL,
            PRIMARY KEY (blob_id, holder_kind, holder_id)
        );
        CREATE TABLE IF NOT EXISTS {schema}.blob_bytes (
            storage_key TEXT PRIMARY KEY,
            bytes BYTEA NOT NULL
        );
        CREATE TABLE IF NOT EXISTS {schema}.blob_references (
            id BIGSERIAL PRIMARY KEY,
            blob_id TEXT NOT NULL REFERENCES {schema}.blobs (id),
            event_sequence BIGINT,
            command_id BIGINT,
            field TEXT NOT NULL,
            CHECK ((event_sequence IS NULL) <> (command_id IS NULL))
        );
        CREATE INDEX IF NOT EXISTS blob_references_by_blob
            ON {schema}.blob_references (blob_id, id)"
    )
}

/// The blob tables, for a bounded context provisioned before they existed.
/// Checked in the catalog first, so a startup that finds them takes no
/// lock (docs/architecture.md §158).
pub async fn ensure_blob_tables(pool: &Pool, bounded_context: &str) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let (present,): (bool,) = sqlx::query_as(
        "SELECT to_regclass($1) IS NOT NULL AND to_regclass($2) IS NOT NULL \
         AND to_regclass($3) IS NOT NULL AND to_regclass($4) IS NOT NULL",
    )
    .bind(format!("{schema}.blobs"))
    .bind(format!("{schema}.blob_bytes"))
    .bind(format!("{schema}.blob_references"))
    .bind(format!("{schema}.blob_pins"))
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
            let deleted = sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {schema}.blob_bytes WHERE storage_key = $1"
            )))
            .bind(storage_key)
            .execute(&self.pool)
            .await;
            match deleted {
                Ok(_) => Ok(()),
                // The bounded context was deleted: the bytes went with its
                // schema, and something already gone is not an error.
                Err(sqlx::Error::Database(e))
                    if matches!(e.code().as_deref(), Some("42P01") | Some("3F000")) =>
                {
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
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
    /// The `(subject_key, subject_value)` whose key encrypts the bytes.
    pub subject: Option<(String, String)>,
}

type BlobRow = (
    String,
    String,
    i64,
    String,
    String,
    String,
    DateTime<Utc>,
    Option<String>,
    Option<String>,
);

const BLOB_COLUMNS: &str =
    "id, sha256, size, media_type, storage_key, state, created_at, subject_key, subject_value";

fn blob_record(row: BlobRow) -> BlobRecord {
    let (id, sha256, size, media_type, storage_key, state, created_at, subject_key, subject_value) =
        row;
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
        subject: subject_key.zip(subject_value),
    }
}

/// Who a new blob's bytes are encrypted for: the subject's data key is
/// provisioned if it doesn't exist yet, as for a sensitive field.
#[derive(Clone, Copy)]
pub struct BlobSubject<'a> {
    /// The subject's namespace, e.g. `customer`.
    pub subject_key: &'a str,
    /// The subject's identifier.
    pub subject_value: &'a str,
    /// The key service holding the master key.
    pub key_service: &'a KeyService,
}

/// `bytes` cut into frames sealed under `data_key`. A frame is sealed only
/// once it is known whether more follows, so the last one says so.
fn encrypting_stream<'a>(
    bytes: ByteStream<'a>,
    data_key: DataKey,
    blob_id: String,
) -> ByteStream<'a> {
    let frame = crate::encryption::BLOB_FRAME_PLAINTEXT;
    futures_util::stream::unfold(
        (bytes, Vec::<u8>::new(), 0_u64, false),
        move |(mut bytes, mut buffer, index, done)| {
            let data_key = data_key.clone();
            let blob_id = blob_id.clone();
            async move {
                if done {
                    return None;
                }
                let mut ended = false;
                while buffer.len() <= frame {
                    match bytes.next().await {
                        Some(Ok(chunk)) => buffer.extend_from_slice(&chunk),
                        Some(Err(e)) => return Some((Err(e), (bytes, Vec::new(), index, true))),
                        None => {
                            ended = true;
                            break;
                        }
                    }
                }
                let rest = buffer.split_off(buffer.len().min(frame));
                let last = ended && rest.is_empty();
                let sealed =
                    crate::encryption::seal_blob_frame(&data_key, &blob_id, index, last, &buffer)
                        .map_err(std::io::Error::other);
                Some((sealed, (bytes, rest, index + 1, last)))
            }
        },
    )
    .boxed()
}

/// Plaintext from a stream of a blob's stored frames. Each frame's tag is
/// checked before any of it is served, and the last frame isn't served until
/// `inner` has ended cleanly: that end is where a verifying stream reports a
/// wrong hash or size, and nothing may follow the last frame.
fn decrypting_stream(
    inner: ByteStream<'static>,
    data_key: DataKey,
    blob_id: String,
    stored_size: u64,
) -> ByteStream<'static> {
    let frame = crate::encryption::BLOB_FRAME_PLAINTEXT + crate::encryption::BLOB_FRAME_OVERHEAD;
    let frames = crate::encryption::blob_frame_count(stored_size);
    futures_util::stream::unfold(
        // `Some(inner)` until it has ended: it must not be polled after that.
        (Some(inner), Vec::<u8>::new(), 0_u64, false),
        move |(mut inner, mut buffer, index, done)| {
            let data_key = data_key.clone();
            let blob_id = blob_id.clone();
            async move {
                if done || index >= frames {
                    return None;
                }
                let last = index + 1 == frames;
                // For the last frame, read on to the end of the stream.
                while last || buffer.len() < frame {
                    let Some(stream) = inner.as_mut() else {
                        break;
                    };
                    match stream.next().await {
                        Some(Ok(chunk)) => buffer.extend_from_slice(&chunk),
                        Some(Err(e)) => return Some((Err(e), (None, Vec::new(), index, true))),
                        None => inner = None,
                    }
                }
                let rest = buffer.split_off(buffer.len().min(frame));
                if last && !rest.is_empty() {
                    let trailing = std::io::Error::other(format!(
                        "blob {blob_id} has bytes after its last frame"
                    ));
                    return Some((Err(trailing), (None, Vec::new(), index, true)));
                }
                match crate::encryption::open_blob_frame(&data_key, &blob_id, index, last, &buffer)
                {
                    Ok(plain) => Some((Ok(plain), (inner, rest, index + 1, false))),
                    Err(e) => Some((Err(std::io::Error::other(e)), (None, rest, index, true))),
                }
            }
        },
    )
    .boxed()
}

/// Stores `bytes` in `store` as a new, `pending` blob of `bounded_context`
/// and answers the reference a payload can then carry. Upload first,
/// append second: nothing references it yet, so a failure after the bytes
/// are stored leaves at most bytes no row names, which this removes again
/// where it can. With a `subject`, the store only ever sees frames sealed
/// under that subject's data key, and the reference's `sha256` is of those
/// stored bytes: it must say nothing about the content once the key is
/// gone (docs/architecture.md §239).
#[allow(clippy::too_many_arguments)]
pub async fn upload_blob(
    pool: &Pool,
    store: &dyn BlobStore,
    bounded_context: &str,
    media_type: &str,
    bytes: ByteStream<'_>,
    max_bytes: u64,
    subject: Option<BlobSubject<'_>>,
    now: DateTime<Utc>,
) -> crate::error::Result<BlobRef> {
    let id = crate::shared::try_generate_token_id()?;
    let encrypted = subject.is_some();
    let (subject_key, subject_value) = (
        subject.map(|s| s.subject_key),
        subject.map(|s| s.subject_value),
    );
    let (bytes, store_limit) = match subject {
        Some(subject) => {
            let (_, _, data_key) = get_or_create_encryption_key(
                pool,
                bounded_context,
                subject.subject_key,
                subject.subject_value,
                subject.key_service,
            )
            .await?;
            (
                encrypting_stream(bytes, data_key, id.clone()),
                crate::encryption::blob_stored_size(max_bytes),
            )
        }
        None => (bytes, max_bytes),
    };
    let stored = store
        .put(bounded_context, &id, bytes, store_limit)
        .await
        .map_err(|e| match e {
            // The limit the caller set, not the framed one the store checked.
            crate::error::Error::EventStore(crate::event_store::Error::BlobTooLarge { .. }) => {
                crate::event_store::Error::BlobTooLarge { limit: max_bytes }.into()
            }
            other => other,
        })?;
    let size = if encrypted {
        crate::encryption::blob_plaintext_size(stored.size)
    } else {
        stored.size
    };
    let schema = schema_ident(bounded_context);
    let inserted = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.blobs (id, sha256, size, media_type, storage_key, state, \
         subject_key, subject_value, created_at, grace_from) \
         VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7, $8, $8)"
    )))
    .bind(&id)
    .bind(&stored.sha256)
    .bind(i64::try_from(size).unwrap_or(i64::MAX))
    .bind(media_type)
    .bind(&stored.storage_key)
    .bind(subject_key)
    .bind(subject_value)
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
        size,
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

/// A blob's bytes, streamed from `store`, with the hash and size of the
/// stored bytes checked as they pass, and each frame's tag for a blob with
/// a subject: the stream ends in an error rather than serving bytes that
/// aren't the ones the reference names. `None` when `bounded_context` has
/// no such blob, it was deleted, or its subject was forgotten. Access
/// checks are the caller's ([`blob_access_for_mapping`],
/// [`blob_access_for_token`]).
pub async fn read_blob(
    pool: &Pool,
    store: &dyn BlobStore,
    bounded_context: &str,
    id: &str,
    key_service: Option<&KeyService>,
) -> crate::error::Result<Option<(BlobRef, ByteStream<'static>)>> {
    let Some(record) = get_blob(pool, bounded_context, id).await? else {
        return Ok(None);
    };
    if record.state != Some(BlobState::Pending) && record.state != Some(BlobState::Referenced) {
        return Ok(None);
    }
    let data_key = match &record.subject {
        Some((subject_key, subject_value)) => {
            let key_service = key_service.ok_or(encryption::Error::MasterKeyNotConfigured)?;
            let key = get_active_data_key(
                pool,
                bounded_context,
                subject_key,
                subject_value,
                key_service,
            )
            .await?;
            let Some(key) = key else {
                return Ok(None);
            };
            Some(key)
        }
        None => None,
    };
    let stored_size = match &data_key {
        Some(_) => crate::encryption::blob_stored_size(record.reference.size),
        None => record.reference.size,
    };
    let inner = store.get(bounded_context, &record.storage_key).await?;
    let expected_sha256 = record.reference.sha256.clone();
    let blob_id = record.reference.id.clone();
    let verified_id = blob_id.clone();
    let verified = futures_util::stream::unfold(
        (inner, Some(HashingCounter::new(stored_size))),
        move |(mut inner, counter)| {
            let expected_sha256 = expected_sha256.clone();
            let blob_id = verified_id.clone();
            async move {
                let mut counter = counter?;
                match inner.next().await {
                    Some(Ok(chunk)) => match counter.update(&chunk) {
                        Ok(()) => Some((Ok(chunk), (inner, Some(counter)))),
                        Err(e) => Some((Err(std::io::Error::other(e)), (inner, None))),
                    },
                    Some(Err(e)) => Some((Err(e), (inner, None))),
                    None => {
                        let (size, sha256) = counter.finish();
                        if size == stored_size && sha256 == expected_sha256 {
                            None
                        } else {
                            Some((
                                Err(std::io::Error::other(format!(
                                    "blob {blob_id} does not match its reference: {size} bytes, \
                                     sha256 {sha256}"
                                ))),
                                (inner, None),
                            ))
                        }
                    }
                }
            }
        },
    )
    .boxed();
    let stream = match data_key {
        Some(data_key) => decrypting_stream(verified, data_key, blob_id, stored_size),
        None => verified,
    };
    Ok(Some((record.reference, stream)))
}

/// One blob reference an append is about to store: in a command's payload
/// (`event_sequence: None`) or in the payload of the event at
/// `event_sequence`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BlobClaim {
    pub(super) event_sequence: Option<i64>,
    pub(super) field: String,
    pub(super) reference: BlobRef,
    pub(super) subject: Option<(String, String)>,
}

fn claims(
    fields: &[BlobField],
    payload: &str,
    event_sequence: Option<i64>,
    out: &mut Vec<BlobClaim>,
) -> crate::error::Result<()> {
    for declared in crate::blob::declared_blob_refs(fields, payload)? {
        out.push(BlobClaim {
            event_sequence,
            field: declared.field,
            reference: declared.reference,
            subject: declared.subject,
        });
    }
    Ok(())
}

/// The blob references in `event`'s stored payload, each in a field its
/// type declares (pure - see [`check_blob_claims`] for the rest).
pub(super) fn event_blob_claims(event: &Event) -> crate::error::Result<Vec<BlobClaim>> {
    let mut out = Vec::new();
    claims(
        &event.event_type.blob_fields,
        &event.payload,
        Some(event.sequence),
        &mut out,
    )?;
    Ok(out)
}

/// [`event_blob_claims`] for a command and every event it produced.
pub(super) fn command_blob_claims(
    command: &Command,
    events: &[Event],
) -> crate::error::Result<Vec<BlobClaim>> {
    let mut out = Vec::new();
    claims(
        &command.command_type.blob_fields,
        &command.payload,
        None,
        &mut out,
    )?;
    for event in events {
        out.extend(event_blob_claims(event)?);
    }
    Ok(out)
}

/// Refuses an append whose `claims` name a blob `bounded_context` doesn't
/// have, has deleted, or holds with another size, hash, media type or
/// subject. On
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
        // Whose key protects the bytes has to be whose payload names them:
        // forgetting this payload's subject must erase what it points at.
        if record.subject != claim.subject {
            return refuse("the blob's subject differs from the one this field declares");
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

/// Who holds a pending blob past its grace period: a parked delivery or a
/// pending approval whose stored request will be replayed later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BlobPinHolder {
    /// A parked delivery's stored request, by the row's id.
    ParkedDelivery,
    /// A command proposal's payload, by its id.
    CommandProposal,
}

impl BlobPinHolder {
    fn as_str(self) -> &'static str {
        match self {
            BlobPinHolder::ParkedDelivery => "parked_delivery",
            BlobPinHolder::CommandProposal => "command_proposal",
        }
    }
}

/// The ids of the blobs `document` references. Nothing to pin, and no query
/// for one, when it references none.
pub(super) fn referenced_blob_ids(document: &str) -> crate::error::Result<Vec<String>> {
    let found = crate::blob::find_blob_refs(document)
        .map_err(|e| crate::event_store::Error::PayloadDecodeFailed(e.to_string()))?;
    Ok(found
        .into_iter()
        .filter_map(|found| found.reference.ok().map(|r| r.id))
        .collect())
}

/// The first half of pinning, before the holder's row is written: locks the
/// blobs in id order, so two holders of the same blobs can't deadlock, and
/// restarts a pending one's grace period. Writing the row is what stops
/// collection: its `FOR UPDATE SKIP LOCKED` passes over a row locked here,
/// and one that got past its snapshot before this commits re-checks
/// `grace_from` and no longer qualifies. A pin alone wouldn't: collection's
/// snapshot can predate it.
pub(super) async fn hold_blobs_for_pin(
    tx: &mut Transaction<'_, Postgres>,
    bounded_context: &str,
    ids: &[String],
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "WITH held AS (SELECT id FROM {schema}.blobs WHERE id = ANY($1) ORDER BY id FOR UPDATE) \
         UPDATE {schema}.blobs b SET grace_from = $2 FROM held \
         WHERE b.id = held.id AND b.state = 'pending'"
    )))
    .bind(ids)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The second half, once the holder's row is written: in the same
/// transaction, so there is never a holder without its pin, or the reverse.
/// A reference to a blob this bounded context doesn't have pins nothing;
/// the replay refuses it.
pub(super) async fn pin_held_blobs(
    tx: &mut Transaction<'_, Postgres>,
    bounded_context: &str,
    holder: BlobPinHolder,
    holder_id: &str,
    ids: &[String],
) -> crate::error::Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.blob_pins (blob_id, holder_kind, holder_id) \
         SELECT id, $2, $3 FROM {schema}.blobs WHERE id = ANY($1) \
         ON CONFLICT DO NOTHING"
    )))
    .bind(ids)
    .bind(holder.as_str())
    .bind(holder_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Makes a subject's blobs `deleted`, as `forgetSubject` does: refused by
/// every later append and read at once, whatever pins them. Collection
/// then removes the bytes ([`collect_blobs`]), so this also reaches a store
/// outside Postgres.
pub async fn forget_subject_in_blobs(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<u64> {
    let schema = schema_ident(bounded_context);
    Ok(sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.blobs SET state = 'deleted', deleted_at = $3 \
         WHERE subject_key = $1 AND subject_value = $2 AND state <> 'deleted'"
    )))
    .bind(subject_key)
    .bind(subject_value)
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected())
}

/// What one [`collect_blobs`] pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CollectedBlobs {
    /// Pending blobs past their grace period, now `deleted`.
    pub orphans: u64,
    /// Blobs whose bytes were removed from the store.
    pub bytes_removed: u64,
    /// Blobs whose bytes the store failed to remove; tried again next pass.
    pub failed: u64,
}

/// One pass of orphan collection (docs/architecture.md §239), at most
/// `limit` blobs of each kind:
///
/// 1. A pin whose holder is gone (a discarded or retried parked delivery,
///    a resolved proposal) is dropped, and its blob gets a fresh grace
///    period: it may have waited far longer than that already.
/// 2. A pending, unpinned blob past `cutoff` becomes `deleted`. An append
///    checking it holds its row `FOR SHARE`, so one of the two goes first:
///    the append's reference is stored and the blob stops being pending,
///    or the append finds it deleted and refuses.
/// 3. The bytes of `deleted` blobs are removed from the store, whatever
///    made them so. A failure leaves the row to retry.
pub async fn collect_blobs(
    pool: &Pool,
    store: &dyn BlobStore,
    bounded_context: &str,
    cutoff: DateTime<Utc>,
    now: DateTime<Utc>,
    limit: i64,
) -> crate::error::Result<CollectedBlobs> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "WITH dead AS ( \
             DELETE FROM {schema}.blob_pins pin WHERE \
                (pin.holder_kind = 'parked_delivery' AND NOT EXISTS ( \
                    SELECT 1 FROM {schema}.parked_deliveries p \
                    WHERE p.id::text = pin.holder_id)) \
                OR (pin.holder_kind = 'command_proposal' AND NOT EXISTS ( \
                    SELECT 1 FROM {schema}.command_proposals c \
                    WHERE c.id = pin.holder_id AND c.status = 'pending')) \
             RETURNING pin.blob_id \
         ) \
         UPDATE {schema}.blobs SET grace_from = $1 \
         WHERE id IN (SELECT blob_id FROM dead) AND state = 'pending'"
    )))
    .bind(now)
    .execute(pool)
    .await?;
    let orphans = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.blobs SET state = 'deleted', deleted_at = $2 WHERE id IN ( \
             SELECT b.id FROM {schema}.blobs b \
             WHERE b.state = 'pending' AND b.grace_from < $1 \
               AND NOT EXISTS (SELECT 1 FROM {schema}.blob_pins pin WHERE pin.blob_id = b.id) \
             ORDER BY b.grace_from LIMIT $3 FOR UPDATE SKIP LOCKED)"
    )))
    .bind(cutoff)
    .bind(now)
    .bind(limit)
    .execute(pool)
    .await?
    .rows_affected();
    let to_remove: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, storage_key FROM {schema}.blobs \
         WHERE state = 'deleted' AND bytes_removed_at IS NULL LIMIT $1"
    )))
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let mut collected = CollectedBlobs {
        orphans,
        ..CollectedBlobs::default()
    };
    for (id, storage_key) in to_remove {
        if let Err(e) = store.delete(bounded_context, &storage_key).await {
            tracing::warn!(
                bounded_context = %bounded_context,
                blob = %id,
                error = %e,
                "a deleted blob's bytes could not be removed; trying again next pass"
            );
            collected.failed += 1;
            continue;
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.blobs SET bytes_removed_at = $2 WHERE id = $1"
        )))
        .bind(&id)
        .bind(now)
        .execute(pool)
        .await?;
        collected.bytes_removed += 1;
    }
    Ok(collected)
}

/// Queues the bytes of `bounded_context`'s blobs for removal, in the
/// transaction that drops its schema: afterwards nothing else names them.
/// A store outside Postgres deletes them later ([`collect_deleted_contexts`]);
/// for [`PostgresBlobStore`] they went with the schema, and its delete
/// finds nothing. Keyed by the random storage key, so a new bounded context
/// taking the name loses nothing.
pub(super) async fn queue_blob_deletions(
    tx: &mut Transaction<'_, Postgres>,
    bounded_context: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let (present,): (bool,) = sqlx::query_as("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("{schema}.blobs"))
        .fetch_one(&mut **tx)
        .await?;
    if !present {
        return Ok(());
    }
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO blob_deletions (bounded_context, storage_key, requested_at) \
         SELECT $1, storage_key, $2 FROM {schema}.blobs WHERE bytes_removed_at IS NULL \
         ON CONFLICT DO NOTHING"
    )))
    .bind(bounded_context)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Removes at most `limit` blobs' bytes that a deleted bounded context
/// left behind, answering how many. Survives a crash: a row goes only once
/// its bytes have.
pub async fn collect_deleted_contexts(
    pool: &Pool,
    store: &dyn BlobStore,
    limit: i64,
) -> crate::error::Result<u64> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT bounded_context, storage_key FROM blob_deletions \
         ORDER BY requested_at LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    let mut removed = 0;
    for (bounded_context, storage_key) in rows {
        if let Err(e) = store.delete(&bounded_context, &storage_key).await {
            tracing::warn!(
                bounded_context = %bounded_context,
                error = %e,
                "a deleted bounded context's blob could not be removed; trying again next pass"
            );
            continue;
        }
        sqlx::query("DELETE FROM blob_deletions WHERE bounded_context = $1 AND storage_key = $2")
            .bind(&bounded_context)
            .bind(&storage_key)
            .execute(pool)
            .await?;
        removed += 1;
    }
    Ok(removed)
}

/// How many references [`blob_access_for_mapping`] and
/// [`blob_access_for_token`] look through per round trip.
const ACCESS_PAGE: i64 = 100;

/// One page of where a blob is referenced, after `after`.
async fn blob_reference_page(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
    after: i64,
) -> crate::error::Result<Vec<(i64, Option<i64>, Option<i64>)>> {
    let schema = schema_ident(bounded_context);
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, event_sequence, command_id FROM {schema}.blob_references \
         WHERE blob_id = $1 AND id > $2 ORDER BY id LIMIT $3"
    )))
    .bind(id)
    .bind(after)
    .bind(ACCESS_PAGE)
    .fetch_all(pool)
    .await?)
}

/// Whether a rendered payload still shows a reference to blob `id`, which
/// is what "with that field visible" comes down to: rendering is where
/// private fields are redacted, so this is the same answer a read gives.
fn rendered_shows_blob(rendered: &str, id: &str) -> bool {
    crate::blob::find_blob_refs(rendered).is_ok_and(|found| {
        found
            .iter()
            .any(|f| f.reference.as_ref().is_ok_and(|r| r.id == id))
    })
}

/// Whether a Role with `mapping` in the blob's bounded context may download
/// it (docs/architecture.md §239): it can read at least one event or
/// command referencing the blob with that field visible - owner scope,
/// private-field grants - and, for a blob with a subject, may read that
/// subject's sensitive fields. A blob nothing references yet is nobody's to
/// read. The caller answers a refusal as it answers a blob that doesn't
/// exist.
pub async fn blob_access_for_mapping(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
    mapping: &RoleAccessMapping,
) -> crate::error::Result<bool> {
    let Some(record) = get_blob(pool, bounded_context, id).await? else {
        return Ok(false);
    };
    if let Some((_, subject_value)) = &record.subject {
        if !crate::event_store::sensitive_field_is_granted(mapping, subject_value) {
            return Ok(false);
        }
    }
    let grants =
        list_active_private_field_grants_for_grantee(pool, bounded_context, &mapping.role).await?;
    let no_key = |_: &str, _: &str| None;
    let mut after = -1;
    loop {
        let page = blob_reference_page(pool, bounded_context, id, after).await?;
        let Some(last) = page.last() else {
            return Ok(false);
        };
        after = last.0;
        for (_, event_sequence, command_id) in &page {
            let shown = if let Some(sequence) = event_sequence {
                get_event_by_sequence(pool, bounded_context, *sequence)
                    .await?
                    .filter(|event| {
                        crate::event_store::event_owner_scope_satisfied(
                            event,
                            mapping.scope.as_deref(),
                        )
                    })
                    .map(|event| {
                        crate::event_store::render_event(&event, mapping, &no_key, &grants)
                    })
            } else if let Some(command_id) = command_id {
                get_command_by_id(pool, bounded_context, *command_id)
                    .await?
                    .filter(|command| {
                        crate::event_store::command_owner_scope_satisfied(
                            command,
                            mapping.scope.as_deref(),
                        )
                    })
                    .map(|command| {
                        crate::event_store::render_command(&command, mapping, &no_key, &grants)
                    })
            } else {
                None
            };
            if shown.is_some_and(|rendered| rendered_shows_blob(&rendered, id)) {
                return Ok(true);
            }
        }
    }
}

/// Whether an event read token may download the blob: an event of the
/// token's own type references it with that field visible, under the REST
/// track's redaction. REST reads fail closed on private fields and never
/// decrypt (docs/architecture.md §31), so a private field's blob and a
/// blob with a subject are refused.
pub async fn blob_access_for_token(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
    token: &crate::access_control::EventReadToken,
) -> crate::error::Result<bool> {
    let Some(record) = get_blob(pool, bounded_context, id).await? else {
        return Ok(false);
    };
    if record.subject.is_some() || !token.event_type.event_read_allowed {
        return Ok(false);
    }
    let mut after = -1;
    loop {
        let page = blob_reference_page(pool, bounded_context, id, after).await?;
        let Some(last) = page.last() else {
            return Ok(false);
        };
        after = last.0;
        for (_, event_sequence, _) in &page {
            let Some(sequence) = event_sequence else {
                continue;
            };
            let Some(event) = get_event_by_sequence(pool, bounded_context, *sequence).await? else {
                continue;
            };
            if event.event_type.name == token.event_type.name
                && crate::event_store::event_owner_scope_satisfied(&event, token.scope.as_deref())
                && rendered_shows_blob(
                    &crate::event_store::redact_private_fields(&event).payload,
                    id,
                )
            {
                return Ok(true);
            }
        }
    }
}
