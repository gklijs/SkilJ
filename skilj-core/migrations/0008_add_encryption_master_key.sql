-- The master key version in use and where its rotation stands
-- (docs/architecture.md §230). One row, written the first time a process
-- with a key provider starts. The history is in the admin bounded
-- context's EncryptionMasterKeyRotated, EncryptionDataKeysRewrapped and
-- EncryptionMasterKeyRetired events.
CREATE TABLE encryption_master_key (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    -- The key provider's name (KeyProvider::name).
    provider TEXT NOT NULL,
    -- The version new data keys are wrapped under, and every data key is
    -- rewrapped to.
    version INTEGER NOT NULL,
    rotated_at TIMESTAMPTZ NOT NULL,
    -- Whether the versions below retire as soon as the rewrap is done,
    -- rather than after the grace period.
    retire_immediately BOOLEAN NOT NULL DEFAULT false,
    -- When every data key was found wrapped under `version`; NULL while
    -- the rewrap is pending.
    rewrapped_at TIMESTAMPTZ,
    -- Every version below this one is retired at the provider.
    retired_below INTEGER NOT NULL DEFAULT 0
);
