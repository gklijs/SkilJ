SELECT pg_advisory_xact_lock(hashtext(@LOCK_KEY@)::bigint);
DO $patch$
DECLARE
    kind "char";
    probes BOOLEAN;
BEGIN
    SELECT c.relkind INTO kind FROM pg_class c
    WHERE c.oid = to_regclass('@SCHEMA@.event_tags');
    -- Whether the trigger function already there is the one below. The one
    -- before it filtered with a sub-select PostgreSQL may hash, and a bounded
    -- context indexed by it has to get this one (docs/architecture.md §245).
    SELECT p.prosrc NOT LIKE '%NOT EXISTS%' INTO probes FROM pg_proc p
    WHERE p.oid = to_regprocedure('@SCHEMA@.index_event_tags()');
    IF kind = 'v' AND probes THEN RETURN; END IF;

    -- The first statement adds the tags the dictionary doesn't have yet. It
    -- looks each one up with a scalar sub-select, which PostgreSQL answers
    -- with an index probe. As an EXISTS test the planner could, and for
    -- any dictionary of up to some tens of thousands of tags did, answer it
    -- by hashing the whole dictionary: one sequential scan of
    -- event_tag_values for every inserted event (docs/architecture.md §245).
    -- Created before the tables it names, which PL/pgSQL allows, so that
    -- replacing it for an indexed bounded context locks nothing.
    CREATE OR REPLACE FUNCTION @SCHEMA@.index_event_tags() RETURNS trigger
    LANGUAGE plpgsql AS $index$
    BEGIN
        INSERT INTO @SCHEMA@.event_tag_values(tag_key, tag_value)
            SELECT k, v FROM (
                SELECT DISTINCT t->>'key' k, t->>'value' v
                FROM new_events n, jsonb_array_elements(n.tags) t
            ) incoming
            WHERE CASE WHEN v IS NULL THEN (
                SELECT d.id FROM @SCHEMA@.event_tag_values d WHERE d.tag_key=k AND d.tag_value IS NULL
            ) ELSE (
                SELECT d.id FROM @SCHEMA@.event_tag_values d WHERE d.tag_key=k AND d.tag_value=v
            ) END IS NULL
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
    IF kind = 'v' THEN RETURN; END IF;

    -- Fence every event writer while rebuilding the derived index. The
    -- source of truth is events.tags, including historical null values.
    LOCK TABLE @SCHEMA@.events IN SHARE ROW EXCLUSIVE MODE;
    CREATE TABLE IF NOT EXISTS @SCHEMA@.event_tag_values (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        tag_key TEXT COLLATE "C" NOT NULL,
        tag_value TEXT COLLATE "C",
        UNIQUE NULLS NOT DISTINCT (tag_key, tag_value)
    );
    CREATE TABLE @SCHEMA@.event_tag_refs (
        -- This is a derived index, populated only after dictionary
        -- resolution in the same transaction. Avoid one FK probe/row
        -- lock per reference on the append hot path.
        tag_id BIGINT NOT NULL,
        event_type_name TEXT NOT NULL,
        sequence BIGINT NOT NULL
    );
    CREATE OR REPLACE FUNCTION @SCHEMA@.event_tag_id(TEXT, TEXT) RETURNS BIGINT
    LANGUAGE sql STABLE AS $id$
        SELECT id FROM @SCHEMA@.event_tag_values WHERE tag_key = $1 AND tag_value = $2
        UNION ALL
        SELECT id FROM @SCHEMA@.event_tag_values WHERE tag_key = $1 AND tag_value IS NULL AND $2 IS NULL
    $id$;
    INSERT INTO @SCHEMA@.event_tag_values(tag_key, tag_value)
        SELECT DISTINCT t->>'key', t->>'value'
        FROM @SCHEMA@.events e, jsonb_array_elements(e.tags) t
        ORDER BY 1, 2 NULLS FIRST
        ON CONFLICT (tag_key, tag_value) DO NOTHING;
    INSERT INTO @SCHEMA@.event_tag_refs
        SELECT @SCHEMA@.event_tag_id(t->>'key', t->>'value'), e.event_type_name, e.sequence
        FROM @SCHEMA@.events e, jsonb_array_elements(e.tags) t;
    CREATE INDEX event_tag_refs_lookup ON @SCHEMA@.event_tag_refs
        (tag_id, event_type_name, sequence);

    IF kind IS NOT NULL THEN DROP TABLE @SCHEMA@.event_tags; END IF;
    CREATE VIEW @SCHEMA@.event_tags AS
        SELECT v.tag_key, v.tag_value, r.event_type_name, r.sequence, r.tag_id
        FROM @SCHEMA@.event_tag_refs r JOIN @SCHEMA@.event_tag_values v ON v.id = r.tag_id;

    DROP TRIGGER IF EXISTS events_index_tags ON @SCHEMA@.events;
    CREATE TRIGGER events_index_tags AFTER INSERT ON @SCHEMA@.events
        REFERENCING NEW TABLE AS new_events
        FOR EACH STATEMENT EXECUTE FUNCTION @SCHEMA@.index_event_tags();

    -- Retain the legacy SQL view for readers and older instances during
    -- rollout. New reads use integer IDs against event_tag_refs directly.
    CREATE OR REPLACE FUNCTION @SCHEMA@.change_event_tag() RETURNS trigger
    LANGUAGE plpgsql AS $change$
    DECLARE new_id BIGINT;
    BEGIN
        IF TG_OP = 'DELETE' THEN
            DELETE FROM @SCHEMA@.event_tag_refs WHERE tag_id = OLD.tag_id
                AND event_type_name = OLD.event_type_name AND sequence = OLD.sequence;
            RETURN OLD;
        END IF;
        INSERT INTO @SCHEMA@.event_tag_values(tag_key, tag_value)
            VALUES (NEW.tag_key, NEW.tag_value) ON CONFLICT (tag_key, tag_value) DO NOTHING;
        new_id := @SCHEMA@.event_tag_id(NEW.tag_key, NEW.tag_value);
        UPDATE @SCHEMA@.event_tag_refs SET tag_id = new_id,
            event_type_name = NEW.event_type_name, sequence = NEW.sequence
            WHERE tag_id = OLD.tag_id AND event_type_name = OLD.event_type_name AND sequence = OLD.sequence;
        NEW.tag_id := new_id;
        RETURN NEW;
    END $change$;
    CREATE TRIGGER event_tags_change INSTEAD OF UPDATE OR DELETE ON @SCHEMA@.event_tags
        FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.change_event_tag();
END $patch$;
