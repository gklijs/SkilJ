-- Capabilities (docs/architecture.md, the section replacing §32's
-- `Role.name` team gate): named privileges a grant holds within its
-- bounded context (`RoleAccessMapping.capabilities`), and the subset an
-- admin grant may hand out to others (`RoleAccessMapping.delegable`).
--
-- NOT NULL with an empty default, so every mapping granted before this
-- column existed holds none and may delegate none - which is also what
-- it could do before capabilities existed. A projection or private field
-- that used to require a `Role.name` is not migrated: the requirement is
-- now a capability nobody holds yet, so it fails closed until a
-- superadmin or a delegating admin grants it.
ALTER TABLE role_access_mappings
    ADD COLUMN capabilities TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN delegable TEXT[] NOT NULL DEFAULT '{}';
