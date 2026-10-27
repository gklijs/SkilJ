-- The encryption master keys in use and where each one's rotation stands
-- (docs/architecture.md §230): the deployment's default key, and the keys
-- of bounded contexts a superadmin gave one of their own. The history is
-- in the admin bounded context's EncryptionMasterKeyRotated,
-- EncryptionDataKeysRewrapped, EncryptionMasterKeyRetired,
-- EncryptionKeyAssigned and EncryptionKeyDeleted events.
CREATE TABLE encryption_master_keys (
    -- The key's provider name (KeyProvider::name), as stored next to every
    -- data key it wraps.
    provider TEXT PRIMARY KEY,
    -- The key's name at the provider for a bounded context's own key;
    -- NULL for the default key.
    key TEXT,
    -- The version new data keys are wrapped under, and every data key is
    -- rewrapped to.
    version INTEGER NOT NULL,
    rotated_at TIMESTAMPTZ NOT NULL,
    -- Whether the versions below retire as soon as the rewrap is done,
    -- rather than after the grace period.
    retire_immediately BOOLEAN NOT NULL DEFAULT false,
    -- When every data key of its bounded contexts was found wrapped under
    -- `version`; NULL while the rewrap is pending.
    rewrapped_at TIMESTAMPTZ,
    -- Every version below this one is retired at the provider.
    retired_below INTEGER NOT NULL DEFAULT 0,
    -- Whether skilj created the key, and so deletes it once no bounded
    -- context uses it; a key a superadmin brought is only let go.
    created_by_skilj BOOLEAN NOT NULL DEFAULT false,
    -- When an unused bounded-context key is deleted (or let go): the
    -- grace period after its last bounded context stopped using it.
    delete_after TIMESTAMPTZ
);

-- Bounded contexts with a key of their own; every other one uses the
-- default key.
CREATE TABLE encryption_key_assignments (
    bounded_context TEXT PRIMARY KEY,
    key TEXT NOT NULL,
    provider TEXT NOT NULL,
    assigned_at TIMESTAMPTZ NOT NULL
);
