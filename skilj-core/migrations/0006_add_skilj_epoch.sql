-- The last database epoch (system identifier and timeline) a skilj
-- instance saw, so a promotion or a point-in-time restore - after which
-- positions held outside the database can be stale - is reported once
-- (docs/architecture.md §176). One row.
CREATE TABLE skilj_epoch (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    epoch TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL
);
