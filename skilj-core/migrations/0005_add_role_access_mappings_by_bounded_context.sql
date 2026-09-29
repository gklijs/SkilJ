-- A bounded context's own active grants, read whenever one is returned
-- over GraphQL (`boundedContexts` returns every context with its grants).
-- The only existing indexes lead with `role_id`, so reading one context's
-- grants scanned every grant in the deployment (docs/architecture.md §126).
CREATE INDEX role_access_mappings_by_bounded_context
    ON role_access_mappings (bounded_context) WHERE status = 'active';
