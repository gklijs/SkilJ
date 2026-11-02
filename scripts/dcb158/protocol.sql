-- Investigation-only protocol. This is NOT a skilj migration.
-- Restricted query language: OR of exact account tags, all event types.
CREATE SCHEMA dcb158;
CREATE TABLE dcb158.counters (name text PRIMARY KEY, value bigint NOT NULL);
INSERT INTO dcb158.counters VALUES ('append', 0), ('publish', 0);
CREATE SEQUENCE dcb158.gapped;
CREATE TABLE dcb158.guards (tag text PRIMARY KEY, revision bigint NOT NULL, last_event uuid);
CREATE TABLE dcb158.commands (request text PRIMARY KEY, id uuid NOT NULL, event_id uuid NOT NULL);
CREATE TABLE dcb158.events (
    id uuid PRIMARY KEY, position bigint, amount bigint NOT NULL, payload jsonb NOT NULL
);
CREATE TABLE dcb158.tags (
    tag text NOT NULL, revision bigint NOT NULL, event_id uuid NOT NULL REFERENCES dcb158.events,
    predecessor uuid REFERENCES dcb158.events,
    PRIMARY KEY (tag, revision), UNIQUE (tag, event_id)
);
CREATE INDEX tags_by_event ON dcb158.tags(event_id);
CREATE TABLE dcb158.outbox (event_id uuid PRIMARY KEY REFERENCES dcb158.events, published boolean NOT NULL DEFAULT false);
CREATE INDEX pending ON dcb158.outbox (event_id) WHERE NOT published;
CREATE TABLE dcb158.batches (batch bigint PRIMARY KEY, size integer NOT NULL, wal_high pg_lsn);
CREATE TABLE dcb158.publication (
    batch bigint NOT NULL REFERENCES dcb158.batches,
    offset_in_batch integer NOT NULL,
    event_id uuid UNIQUE NOT NULL REFERENCES dcb158.events,
    PRIMARY KEY (batch, offset_in_batch)
);

CREATE FUNCTION dcb158.read_state(keys text[])
RETURNS TABLE(revisions bigint[], balance bigint)
LANGUAGE sql SET search_path = dcb158, pg_catalog AS $$
    SELECT ARRAY(SELECT coalesce(g.revision, 0) FROM unnest(keys) WITH ORDINALITY k(tag, n)
                 LEFT JOIN guards g USING(tag) ORDER BY k.n),
           coalesce((SELECT sum(e.amount) FROM events e WHERE e.id IN
                     (SELECT t.event_id FROM tags t WHERE t.tag = ANY(keys))), 0)::bigint
$$;

-- All modes do the same reads, command/event/tag/outbox writes and durable
-- commit. 'serial' also claims a gapless global number; 'uuid-global' keeps
-- global exclusion but drops its counter; 'uuid' drops global exclusion;
-- 'gapped' uses the same concurrent protocol plus a normal numeric sequence.
CREATE FUNCTION dcb158.append(
    mode text, keys text[], expected bigint[], request_key text, delta bigint, cap bigint,
    decision_balance bigint DEFAULT NULL
) RETURNS text LANGUAGE plpgsql SET search_path = dcb158, pg_catalog AS $$
DECLARE
    lock_key integer;
    key text;
    actual bigint[];
    event_id uuid;
    command_id uuid;
    pos bigint;
    balance bigint;
BEGIN
    IF mode NOT IN ('serial', 'uuid-global', 'uuid', 'gapped') THEN
        RAISE EXCEPTION 'unknown mode';
    END IF;
    IF cardinality(keys) = 0 OR cardinality(keys) <> cardinality(expected)
       OR cardinality(keys) <> (SELECT count(DISTINCT k) FROM unnest(keys) k)
       OR array_position(keys, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'keys must be distinct, non-null and nonempty';
    END IF;
    -- Idempotency has its own namespace and precedes all other locks.
    PERFORM pg_advisory_xact_lock(160, hashtext(request_key));
    IF EXISTS (SELECT 1 FROM commands WHERE request = request_key) THEN RETURN 'duplicate'; END IF;
    IF mode IN ('serial', 'uuid-global') THEN
        PERFORM pg_advisory_xact_lock(159, 0);
    END IF;
    -- Hash collisions add false conflicts only. Acquire actual lock IDs in
    -- numeric order, not tag order, to avoid multi-tag acquisition deadlocks.
    FOR lock_key IN SELECT DISTINCT hashtext(k) FROM unnest(keys) k ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(158, lock_key);
    END LOOP;
    INSERT INTO guards SELECT k, 0, NULL FROM unnest(keys) k ON CONFLICT DO NOTHING;
    actual := ARRAY(SELECT g.revision FROM unnest(keys) WITH ORDINALITY k(tag,n)
                    JOIN guards g USING(tag) ORDER BY n);
    IF actual <> expected THEN RETURN 'retry'; END IF;
    -- The initial balance and revisions were read in one SQL snapshot.
    -- Unchanged revisions make the initial decision reusable. Only a
    -- conflicting command needs a new fold while these tag locks are held.
    balance := decision_balance;
    IF balance IS NULL THEN SELECT s.balance INTO balance FROM read_state(keys) s; END IF;
    IF balance + delta > cap THEN RETURN 'rejected'; END IF;

    event_id := uuidv7();
    command_id := uuidv7();
    IF mode = 'serial' THEN
        UPDATE counters SET value = value + 1 WHERE name = 'append' RETURNING value INTO pos;
    ELSIF mode = 'gapped' THEN
        pos := nextval('gapped');
    END IF;
    INSERT INTO commands VALUES (request_key, command_id, event_id);
    INSERT INTO events VALUES (event_id, pos, delta, jsonb_build_object('amount', delta, 'padding', repeat('x', 512)));
    INSERT INTO tags SELECT tag, revision + 1, event_id, last_event FROM guards WHERE tag = ANY(keys);
    UPDATE guards SET revision = revision + 1, last_event = event_id WHERE tag = ANY(keys);
    INSERT INTO outbox VALUES (event_id, false);
    RETURN 'accepted';
END
$$;

-- Synchronous group-commit control: same per-command protocol and global
-- counter, but one durable transaction for the queued requests. On conflict
-- the fixed SQL decision is rerun on fresh state, in array order.
CREATE FUNCTION dcb158.append_batch(items jsonb, mode text DEFAULT 'serial') RETURNS text[]
LANGUAGE plpgsql SET search_path = dcb158, pg_catalog AS $$
DECLARE
    item jsonb;
    keys text[];
    expected bigint[];
    result text;
    fresh_balance bigint;
    lock_key integer;
    results text[] := ARRAY[]::text[];
BEGIN
    -- A batch acquires the UNION of its request/tag locks before processing
    -- any item. Per-item canonical ordering alone would still deadlock
    -- between two multi-command batches whose lock sets overlap.
    FOR lock_key IN SELECT DISTINCT hashtext(j->>'request') FROM jsonb_array_elements(items) j ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(160, lock_key);
    END LOOP;
    IF mode = 'serial' THEN PERFORM pg_advisory_xact_lock(159, 0); END IF;
    FOR lock_key IN SELECT DISTINCT hashtext(tag) FROM jsonb_array_elements(items) j,
        LATERAL jsonb_array_elements_text(j->'keys') tag ORDER BY 1 LOOP
        PERFORM pg_advisory_xact_lock(158, lock_key);
    END LOOP;
    FOR item IN SELECT value FROM jsonb_array_elements(items) LOOP
        keys := ARRAY(SELECT jsonb_array_elements_text(item->'keys'));
        expected := ARRAY(SELECT jsonb_array_elements_text(item->'expected')::bigint);
        result := append(mode, keys, expected, item->>'request', 1, 4611686018427387903,
                         (item->>'balance')::bigint);
        IF result = 'retry' THEN
            SELECT s.revisions, s.balance INTO expected, fresh_balance FROM read_state(keys) s;
            result := append(mode, keys, expected, item->>'request', 1, 4611686018427387903, fresh_balance);
        END IF;
        results := array_append(results, result);
    END LOOP;
    RETURN results;
END
$$;

-- An asynchronous global publication order, topologically extending the
-- tag-local predecessor relation. Acceptance and publication are separate.
-- One global lock amortized over n ready events, not one per ingest commit.
CREATE FUNCTION dcb158.publish(n integer) RETURNS integer
LANGUAGE plpgsql SET search_path = dcb158, pg_catalog AS $$
DECLARE
    ids uuid[];
    high bigint;
BEGIN
    SELECT value INTO high FROM counters WHERE name = 'publish' FOR UPDATE;
    ids := ARRAY(
        SELECT o.event_id FROM outbox o WHERE NOT o.published
        AND NOT EXISTS (
            SELECT 1 FROM tags t WHERE t.event_id = o.event_id AND t.predecessor IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM publication p WHERE p.event_id = t.predecessor)
        ) ORDER BY o.event_id LIMIT n
    );
    IF cardinality(ids) = 0 THEN RETURN 0; END IF;
    INSERT INTO batches(batch, size) VALUES (high + 1, cardinality(ids));
    INSERT INTO publication SELECT high + 1, ord, id FROM unnest(ids) WITH ORDINALITY AS u(id, ord);
    UPDATE outbox SET published = true WHERE event_id = ANY(ids);
    UPDATE counters SET value = high + 1 WHERE name = 'publish';
    RETURN cardinality(ids);
END
$$;

CREATE FUNCTION dcb158.publish_requests(n integer) RETURNS text[]
LANGUAGE plpgsql SET search_path = dcb158, pg_catalog AS $$
DECLARE count integer;
BEGIN
    count := publish(n);
    IF count = 0 THEN RETURN ARRAY[]::text[]; END IF;
    RETURN ARRAY(SELECT c.request FROM publication p JOIN commands c USING(event_id)
                 WHERE p.batch = (SELECT value FROM counters WHERE name = 'publish')
                 ORDER BY p.offset_in_batch);
END
$$;

-- CDC supplies UUIDs in committed transaction order, NOT UUID order. Make
-- the mapping durable before acknowledging the replication slot. Replaying
-- a chunk is safe and does not change previously assigned batch positions.
CREATE FUNCTION dcb158.publish_decoded(decoded uuid[], high_lsn pg_lsn) RETURNS text[]
LANGUAGE plpgsql SET search_path = dcb158, pg_catalog AS $$
DECLARE ids uuid[]; high bigint;
BEGIN
    SELECT value INTO high FROM counters WHERE name = 'publish' FOR UPDATE;
    ids := ARRAY(SELECT id FROM unnest(decoded) WITH ORDINALITY u(id, ord)
                 WHERE NOT EXISTS (SELECT 1 FROM publication p WHERE p.event_id = id) ORDER BY ord);
    IF cardinality(ids) = 0 THEN RETURN ARRAY[]::text[]; END IF;
    INSERT INTO batches VALUES (high + 1, cardinality(ids), high_lsn);
    INSERT INTO publication SELECT high + 1, ord, id FROM unnest(ids) WITH ORDINALITY u(id, ord);
    UPDATE outbox SET published = true WHERE event_id = ANY(ids);
    UPDATE counters SET value = high + 1 WHERE name = 'publish';
    RETURN ARRAY(SELECT c.request FROM publication p JOIN commands c USING(event_id)
                 WHERE p.batch = high + 1 ORDER BY p.offset_in_batch);
END
$$;
