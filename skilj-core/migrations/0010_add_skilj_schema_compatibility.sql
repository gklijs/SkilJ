-- The schema level the release that last upgraded this database recorded,
-- and the oldest level whose binaries may still run against it
-- (docs/architecture.md §249). Written once every bounded context is
-- patched, and never lowered. A binary whose level is below
-- `min_compatible_level` refuses to start (`schema_too_new`); one at or
-- above it starts even with global migrations it doesn't know, which
-- sqlx would otherwise refuse. One row.
CREATE TABLE skilj_schema_compatibility (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    level BIGINT NOT NULL,
    min_compatible_level BIGINT NOT NULL CHECK (min_compatible_level <= level),
    upgraded_by TEXT NOT NULL,
    upgraded_at TIMESTAMPTZ NOT NULL
);
