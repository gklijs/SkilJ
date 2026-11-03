-- Bytes of a deleted bounded context's blobs still to remove from a blob
-- store outside Postgres (docs/architecture.md §239). Filled in the
-- transaction that drops the schema, drained by the collection task.
CREATE TABLE IF NOT EXISTS blob_deletions (
    bounded_context TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (bounded_context, storage_key)
);
