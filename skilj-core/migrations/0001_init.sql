-- SkilJ's own Postgres schema - embedded via `sqlx::migrate!` and picked
-- up by a consuming application's own migration runner (docs/
-- architecture.md §2.2: "library owns its own bootstrap"). Every
-- Integer-typed column is BIGINT, uniformly, per §2.2.1.
--
-- Only the cross-context tables live here: `roles`, `bounded_contexts`,
-- `role_access_mappings`. Everything scoped to one bounded context
-- (event/command types, events, commands, projections, tokens, cursors,
-- the sequence counter) lives in that context's own Postgres schema
-- instead - `bc_<name>`, dynamically provisioned by
-- `db::provision_bounded_context_schema` when a context is created, not
-- tracked by `sqlx::migrate!` at all. See docs/architecture.md §2.2.2 for
-- the full reasoning (performance isolation and clean hard-deletion via
-- `DROP SCHEMA ... CASCADE`).
--
-- Nothing here has ever been deployed, so schema changes land by editing
-- this file directly rather than stacking a new migration on top - see
-- docs/architecture.md's session notes for why `bounded_contexts`
-- normalises onto `roles` instead of the denormalised columns an earlier
-- pass used before `roles` existed to reference.

CREATE TABLE roles (
    id TEXT PRIMARY KEY,
    external_subject TEXT NOT NULL,
    name TEXT NOT NULL,
    superadmin BOOLEAN NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
    created_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

-- `UniqueActiveExternalSubject`: at most one active Role per
-- external_subject - enforced here too (not only in
-- `access_control::create_role`'s own `existing_roles` scan), the same
-- defence-in-depth a partial unique index gives any of this codebase's
-- other "not exists X where active" invariants.
CREATE UNIQUE INDEX roles_unique_active_external_subject ON roles (external_subject) WHERE status = 'active';

CREATE TABLE bounded_contexts (
    name TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('active', 'archived')),
    created_at TIMESTAMPTZ NOT NULL,
    -- entity ContextCreator: 'superadmin' carries created_by_role_id,
    -- 'system' carries none (see ContextCreator::SystemCreator's own doc
    -- comment - SkilJ itself is the creator, no Role to point at).
    created_by_kind TEXT NOT NULL CHECK (created_by_kind IN ('superadmin', 'system')),
    created_by_role_id TEXT REFERENCES roles (id),
    CHECK (
        (created_by_kind = 'system' AND created_by_role_id IS NULL)
        OR (created_by_kind = 'superadmin' AND created_by_role_id IS NOT NULL)
    )
);

-- entity RoleAccessMapping. No `id` field on the Rust struct itself (see
-- its own doc comment in access_control/mod.rs) - `role_id` +
-- `bounded_context` + the partial unique index below is enough to
-- identify "the" active mapping for a pair without one, the same way
-- `roles_unique_active_external_subject` does for Role. A synthetic
-- `id` column exists anyway (cheap, and every other table in this schema
-- has an obvious natural key except this one) purely for tooling/
-- debugging convenience - application code never reads or binds it.
--
-- Stays in the global schema rather than moving into each bounded
-- context's own `bc_<name>` schema alongside the rest of that context's
-- data: a `RoleAccessMapping` is looked up by `Role` at least as often as
-- by `BoundedContext` (resolving what a caller can do spans every
-- context they hold a grant in), and a fan-out query across N per-context
-- schemas for that is worse than one small, unavoidably shared table.
-- `ON DELETE CASCADE` is what keeps a hard-deleted context's grants from
-- needing a separate cleanup query - see `DeleteBoundedContext` in
-- specs/skilj.allium and `db::hard_delete_bounded_context`.
CREATE TABLE role_access_mappings (
    id BIGSERIAL PRIMARY KEY,
    role_id TEXT NOT NULL REFERENCES roles (id),
    bounded_context TEXT NOT NULL REFERENCES bounded_contexts (name) ON DELETE CASCADE,
    level TEXT NOT NULL CHECK (level IN ('read', 'write', 'admin')),
    can_read_sensitive BOOLEAN NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
    created_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

-- The `not exists RoleAccessMapping{role, bounded_context, status:
-- active}` guard `grant_role_access_mapping` checks in memory, enforced
-- here too - same reasoning as `roles_unique_active_external_subject`.
-- Also what makes `role_id, bounded_context, status = 'active'` a safe,
-- unambiguous `WHERE` clause for revocation without needing `id`: at
-- most one row can ever match it.
CREATE UNIQUE INDEX role_access_mappings_unique_active ON role_access_mappings (role_id, bounded_context) WHERE status = 'active';
CREATE INDEX role_access_mappings_by_role ON role_access_mappings (role_id);

-- `AccessToken`'s own full row now lives in its bounded context's
-- `bc_<name>` schema alongside everything else scoped to that context
-- (see `db::provision_bounded_context_schema`) - but `skilj-rest`'s
-- bearer credential (`Authorization: Bearer <id>.<secret>`) carries only
-- `id`, with no bounded context alongside it to say which schema to look
-- in. This tiny global index is the fix: one row per token, resolved
-- first to learn which schema actually holds it. `ON DELETE CASCADE`
-- keeps a hard-deleted context's tokens from leaving a dangling index
-- entry, the same treatment `role_access_mappings` already gets.
CREATE TABLE access_token_index (
    id TEXT PRIMARY KEY,
    bounded_context TEXT NOT NULL REFERENCES bounded_contexts (name) ON DELETE CASCADE
);
