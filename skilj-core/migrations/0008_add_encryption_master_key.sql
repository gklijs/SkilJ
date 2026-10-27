-- The encryption master key new data keys are wrapped under, by its
-- fingerprint (docs/architecture.md §230): startup refuses a process that
-- doesn't have it, and a rotation moves it to the new key before
-- rewrapping. One row, written the first time a process with a master key
-- starts. The history of changes is in the admin bounded context's
-- EncryptionMasterKeyActivated events.
CREATE TABLE encryption_master_key (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    fingerprint TEXT NOT NULL,
    activated_at TIMESTAMPTZ NOT NULL
);
