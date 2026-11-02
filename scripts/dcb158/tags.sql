-- Component benchmark only: event_tags layout and dictionary costs.
CREATE TABLE raw_tags (tag_key text NOT NULL, tag_value text COLLATE "C" NOT NULL, event_type_name text NOT NULL, sequence bigint NOT NULL);
CREATE INDEX raw_lookup ON raw_tags(tag_key, tag_value, event_type_name, sequence);
CREATE TABLE tag_dictionary (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tag_key text NOT NULL, tag_value text COLLATE "C" NOT NULL, UNIQUE(tag_key, tag_value)
);
CREATE TABLE interned_tags (tag_id bigint NOT NULL, event_type_name text NOT NULL, sequence bigint NOT NULL);
CREATE INDEX interned_lookup ON interned_tags(tag_id, event_type_name, sequence);

-- A literal UTF-8 prefix range, independent of custom vs generic query
-- plans. LIKE $1 can lose its upper/lower index bounds on a generic plan.
CREATE FUNCTION prefix_upper(prefix text) RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE n integer := char_length(prefix); code integer;
BEGIN
    WHILE n > 0 LOOP
        code := ascii(substr(prefix,n,1));
        IF code < 1114111 THEN
            -- Skip the Unicode surrogate range, which is not valid UTF-8.
            code := CASE WHEN code = 55295 THEN 57344 ELSE code + 1 END;
            RETURN left(prefix,n-1) || chr(code);
        END IF;
        n := n - 1;
    END LOOP;
    RETURN NULL;
END
$$;

-- Exact and left-prefix discovery reuse the existing unique B-tree. Two
-- disjoint branches handle prefixes with and without a finite upper bound.
CREATE FUNCTION suggest_tags(key text, prefix text, max_results integer, only_used boolean DEFAULT true)
RETURNS TABLE(tag_id bigint, tag_value text) LANGUAGE sql STABLE AS $$
    WITH candidates AS (
        (SELECT d.id, d.tag_value FROM tag_dictionary d WHERE d.tag_key = key
        AND d.tag_value >= prefix AND d.tag_value < prefix_upper(prefix)
        -- OFFSET 0 keeps this correlated. A generic plan can otherwise
        -- hash the entire reference table before returning twenty tags.
        AND (NOT only_used OR EXISTS (SELECT 1 FROM interned_tags t WHERE t.tag_id = d.id OFFSET 0))
        ORDER BY d.tag_value LIMIT least(greatest(max_results, 0), 100))
        UNION ALL
        (SELECT d.id, d.tag_value FROM tag_dictionary d WHERE d.tag_key = key
        AND prefix_upper(prefix) IS NULL AND d.tag_value >= prefix
        AND (NOT only_used OR EXISTS (SELECT 1 FROM interned_tags t WHERE t.tag_id = d.id OFFSET 0))
        ORDER BY d.tag_value LIMIT least(greatest(max_results, 0), 100))
    )
    SELECT c.id, c.tag_value FROM candidates c
    ORDER BY c.tag_value LIMIT least(greatest(max_results, 0), 100)
$$;

CREATE FUNCTION seed_raw(rows integer, keys integer) RETURNS void LANGUAGE sql AS $$
    INSERT INTO raw_tags SELECT 'account', md5((n % keys)::text) || '-tag',
        CASE n % 3 WHEN 0 THEN 'Deposited' WHEN 1 THEN 'Withdrawn' ELSE 'Opened' END, n
    FROM generate_series(0, rows - 1) n
$$;
CREATE FUNCTION seed_dictionary(keys integer) RETURNS void LANGUAGE sql AS $$
    INSERT INTO tag_dictionary(tag_key, tag_value)
    SELECT 'account', md5(n::text) || '-tag' FROM generate_series(0, keys - 1) n
$$;
CREATE FUNCTION seed_interned(rows integer, keys integer) RETURNS void LANGUAGE sql AS $$
    INSERT INTO interned_tags SELECT d.id,
        CASE n % 3 WHEN 0 THEN 'Deposited' WHEN 1 THEN 'Withdrawn' ELSE 'Opened' END, n
    FROM generate_series(0, rows - 1) n
    JOIN tag_dictionary d ON d.tag_key = 'account' AND d.tag_value = md5((n % keys)::text) || '-tag'
$$;

CREATE FUNCTION probe_raw(probes integer, known_values text[], known_ids bigint[]) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE i integer; result bigint := 0; found bigint;
BEGIN
    FOR i IN 0..probes-1 LOOP
        SELECT coalesce(sum(sequence),0) INTO found FROM (
            SELECT sequence FROM raw_tags WHERE tag_key = 'account'
            AND tag_value = known_values[(i % cardinality(known_values)) + 1] AND event_type_name = 'Deposited'
            ORDER BY sequence DESC LIMIT 10
        ) s;
        result := result + found;
    END LOOP;
    RETURN result;
END
$$;
CREATE FUNCTION probe_interned(probes integer, known_values text[], known_ids bigint[]) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE i integer; result bigint := 0; found bigint;
BEGIN
    FOR i IN 0..probes-1 LOOP
        SELECT coalesce(sum(sequence),0) INTO found FROM (
            SELECT t.sequence FROM interned_tags t WHERE t.tag_id = (
                SELECT d.id FROM tag_dictionary d WHERE d.tag_key = 'account'
                AND d.tag_value = known_values[(i % cardinality(known_values)) + 1]
            ) AND t.event_type_name = 'Deposited' ORDER BY t.sequence DESC LIMIT 10
        ) s;
        result := result + found;
    END LOOP;
    RETURN result;
END
$$;
CREATE FUNCTION probe_cached_id(probes integer, known_values text[], known_ids bigint[]) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE i integer; result bigint := 0; found bigint;
BEGIN
    FOR i IN 0..probes-1 LOOP
        -- Both strings and IDs were prepared before timing. All probe
        -- functions receive both arrays to equalize argument transfer costs.
        SELECT coalesce(sum(sequence),0) INTO found FROM (
            SELECT sequence FROM interned_tags WHERE tag_id = known_ids[(i % cardinality(known_ids)) + 1]
            AND event_type_name = 'Deposited' ORDER BY sequence DESC LIMIT 10
        ) s;
        result := result + found;
    END LOOP;
    RETURN result;
END
$$;
