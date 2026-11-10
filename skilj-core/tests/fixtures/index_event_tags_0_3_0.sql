-- The tag trigger function as skilj 0.3.0 created it, for
-- `skilj-core/tests/tag_indexed_events.rs` to put a bounded context back in
-- that state (docs/architecture.md §245). `@SCHEMA@` is the schema.
CREATE OR REPLACE FUNCTION @SCHEMA@.index_event_tags() RETURNS trigger
LANGUAGE plpgsql AS $index$
BEGIN
    INSERT INTO @SCHEMA@.event_tag_values(tag_key, tag_value)
        SELECT k, v FROM (
            SELECT DISTINCT t->>'key' k, t->>'value' v
            FROM new_events n, jsonb_array_elements(n.tags) t
        ) incoming
        WHERE CASE WHEN v IS NULL THEN NOT EXISTS (
            SELECT 1 FROM @SCHEMA@.event_tag_values d WHERE d.tag_key=k AND d.tag_value IS NULL
        ) ELSE NOT EXISTS (
            SELECT 1 FROM @SCHEMA@.event_tag_values d WHERE d.tag_key=k AND d.tag_value=v
        ) END
        ORDER BY k, v NULLS FIRST
        ON CONFLICT (tag_key, tag_value) DO NOTHING;
    -- A separate statement obtains a fresh READ COMMITTED snapshot
    -- after a concurrent dictionary insertion won ON CONFLICT.
    INSERT INTO @SCHEMA@.event_tag_refs
        WITH resolved AS MATERIALIZED (
            SELECT k, v, CASE WHEN v IS NULL THEN (
                SELECT id FROM @SCHEMA@.event_tag_values d WHERE d.tag_key=k AND d.tag_value IS NULL
            ) ELSE (
                SELECT id FROM @SCHEMA@.event_tag_values d WHERE d.tag_key=k AND d.tag_value=v
            ) END AS id FROM (
                SELECT DISTINCT t->>'key' AS k, t->>'value' AS v
                FROM new_events n, jsonb_array_elements(n.tags) t
            ) tags
        )
        SELECT r.id, n.event_type_name, n.sequence
        FROM new_events n, jsonb_array_elements(n.tags) t
        JOIN resolved r ON r.k = t->>'key' AND coalesce(r.v, '') = coalesce(t->>'value', '')
            AND (r.v IS NULL) = (t->>'value' IS NULL);
    RETURN NULL;
END $index$;
