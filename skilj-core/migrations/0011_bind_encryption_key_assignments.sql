-- An encryption key assignment lives and dies with its bounded context
-- (docs/architecture.md §230). Until now the assignment was keyed by the
-- context's name alone and the tick forgot the assignments of deleted
-- contexts afterwards, so a context created again under a deleted one's
-- name, before a tick, inherited its key. The foreign key below removes
-- the assignment in the deleting transaction.

-- Writers of both tables wait while the cleanup and the constraint go in.
LOCK TABLE bounded_contexts, encryption_key_assignments IN SHARE ROW EXCLUSIVE MODE;

-- One value per context lifetime. A writer that read the context before
-- talking to a key provider compares it when it persists what it prepared,
-- which tells the context it saw from a later one with the same name.
ALTER TABLE bounded_contexts ADD COLUMN generation UUID NOT NULL DEFAULT gen_random_uuid();

-- One value per creation of a key's state row, never changed by an update.
-- A key deleted and created again under the same name has a new one, which
-- a wrap prepared against the earlier key cannot match.
ALTER TABLE encryption_master_keys ADD COLUMN incarnation UUID NOT NULL DEFAULT gen_random_uuid();

-- Assignments whose context is gone are orphans: the deleted context's
-- name is free, so nothing may inherit them. Only those go; an assignment
-- that predates its context's creation (assigned_at < created_at) may
-- belong to a deleted predecessor of the same name, which cannot be told
-- from here: it stays, and startup and the preflight report name it for an
-- operator to check against the assignment history.
DELETE FROM encryption_key_assignments a
WHERE NOT EXISTS (SELECT 1 FROM bounded_contexts c WHERE c.name = a.bounded_context);

ALTER TABLE encryption_key_assignments
    ADD CONSTRAINT encryption_key_assignments_bounded_context_fkey
    FOREIGN KEY (bounded_context) REFERENCES bounded_contexts (name) ON DELETE CASCADE;

-- Deadlines set by the old cleanup may have been counted from an orphan's
-- last use. Every pending deletion of a bounded-context key starts over:
-- the tick sets it again, after the grace period, for a key nothing uses.
UPDATE encryption_master_keys SET delete_after = NULL WHERE key IS NOT NULL;

-- An interruption before startup stamps the new level must still make a
-- binary that predates this migration refuse the database.
UPDATE skilj_schema_compatibility
SET level = GREATEST(level, 2), min_compatible_level = GREATEST(min_compatible_level, 2)
WHERE id = 1;
