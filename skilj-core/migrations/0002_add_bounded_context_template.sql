-- Codeberg issue #13: templated bounded-context tenants
-- (specs/skilj.allium's `BoundedContext.template` field). Nullable - most
-- contexts have no template, matching AddBoundedContext's own path.
-- References `bounded_contexts(name)` rather than an id: this table
-- already uses `name` as its primary key (see 0001_init.sql above).
-- `ON DELETE SET NULL` implements `rule DeleteBoundedContext`'s own
-- ensures clause (a deleted template clears the link on every context
-- templated from it, leaving them as ordinary untemplated contexts) at
-- the database level rather than requiring the application to do it as
-- a separate step - the same cascade `bc_<name>` schema deletion already
-- gets via `DROP SCHEMA ... CASCADE`.
ALTER TABLE bounded_contexts
    ADD COLUMN template TEXT REFERENCES bounded_contexts (name) ON DELETE SET NULL;
