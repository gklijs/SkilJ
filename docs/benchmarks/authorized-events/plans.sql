-- Representative final statement shapes on the benchmark fixture.
-- PREPARE also exercises generic plans after repeated parameterized reads.
\timing on
SELECT version();
SHOW shared_buffers;
SHOW work_mem;
PREPARE type_count(bigint,bigint,text[],text,text) AS
SELECT count(*) FROM bc_orders.events e JOIN bc_orders.event_types et ON et.name=e.event_type_name WHERE e.sequence > $1 AND ($2::bigint IS NULL OR e.sequence <= $2) AND e.event_type_name=ANY($3::text[]) AND $4::text IS NULL AND $5::text IS NULL;
PREPARE owner_count(bigint,bigint,text[],text,text) AS SELECT count(*) FROM bc_orders.events e JOIN bc_orders.event_types et ON et.name=e.event_type_name WHERE e.sequence > $1 AND ($2::bigint IS NULL OR e.sequence <= $2) AND e.event_type_name=ANY($3::text[]) AND $4::text IS NULL AND e.sequence = ANY(ARRAY(
 SELECT r.sequence FROM bc_orders.event_types policy
 JOIN bc_orders.event_tag_values v ON v.tag_key=policy.owner_tag_key AND v.tag_value=$5::text
 JOIN bc_orders.event_tag_refs r ON r.tag_id=v.id AND r.event_type_name=policy.name
 WHERE policy.name=ANY($3::text[]) AND r.sequence>$1 AND ($2::bigint IS NULL OR r.sequence<=$2)
 UNION ALL SELECT free.sequence FROM bc_orders.event_types policy
 JOIN bc_orders.events free ON free.event_type_name=policy.name
 WHERE policy.owner_tag_key IS NULL AND policy.name=ANY($3::text[])
 AND free.sequence>$1 AND ($2::bigint IS NULL OR free.sequence<=$2)));
PREPARE correlation_page(bigint,bigint,text[],text,text,bigint) AS SELECT e.* FROM bc_orders.events e JOIN bc_orders.event_types et ON et.name=e.event_type_name WHERE e.sequence > $1 AND ($2::bigint IS NULL OR e.sequence <= $2) AND $3::text[] IS NOT NULL AND e.metadata_correlation_id=$4 AND $5::text IS NULL ORDER BY e.sequence LIMIT $6;
SET plan_cache_mode=force_custom_plan;
EXPLAIN (ANALYZE,BUFFERS) EXECUTE type_count(-1,NULL,ARRAY['Sparse'],NULL,NULL);
EXPLAIN (ANALYZE,BUFFERS) EXECUTE owner_count(-1,NULL,ARRAY['Owned'],NULL,'tenant-1');
EXPLAIN (ANALYZE,BUFFERS) EXECUTE correlation_page(-1,999999,ARRAY[]::text[],'corr-1',NULL,50);
SET plan_cache_mode=force_generic_plan;
EXPLAIN (ANALYZE,BUFFERS) EXECUTE type_count(-1,NULL,ARRAY['Sparse'],NULL,NULL);
EXPLAIN (ANALYZE,BUFFERS) EXECUTE owner_count(-1,NULL,ARRAY['Owned'],NULL,'tenant-1');
EXPLAIN (ANALYZE,BUFFERS) EXECUTE correlation_page(-1,999999,ARRAY[]::text[],'corr-1',NULL,50);
\echo broad_tag_page
EXPLAIN (ANALYZE,BUFFERS) SELECT e.* FROM bc_orders.events e JOIN bc_orders.event_types et ON et.name=e.event_type_name WHERE e.sequence>-1 AND e.sequence = ANY(ARRAY((SELECT l.sequence FROM unnest(ARRAY(SELECT name FROM bc_orders.event_types)) ty(name)
 CROSS JOIN LATERAL (SELECT DISTINCT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-1' AND tag_value='group-0')
 AND t.event_type_name=ty.name AND t.sequence>-1 AND t.sequence<=999999 ORDER BY t.sequence LIMIT 50) l
 ORDER BY l.sequence LIMIT 50))) ORDER BY e.sequence LIMIT 50;
\echo wide_union_page
EXPLAIN (ANALYZE,BUFFERS) SELECT e.* FROM bc_orders.events e JOIN bc_orders.event_types et ON et.name=e.event_type_name WHERE e.sequence>-1 AND e.sequence = ANY(ARRAY((SELECT l.sequence FROM unnest(ARRAY(SELECT name FROM bc_orders.event_types)) ty(name)
 CROSS JOIN LATERAL (SELECT DISTINCT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-1' AND tag_value='group-0')
 AND t.event_type_name=ty.name AND t.sequence>-1 AND t.sequence<=999999 ORDER BY t.sequence LIMIT 50) l
 ORDER BY l.sequence LIMIT 50) UNION (SELECT l.sequence FROM unnest(ARRAY(SELECT name FROM bc_orders.event_types)) ty(name)
 CROSS JOIN LATERAL (SELECT DISTINCT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-2' AND tag_value='group-0')
 AND t.event_type_name=ty.name AND t.sequence>-1 AND t.sequence<=999999 ORDER BY t.sequence LIMIT 50) l
 ORDER BY l.sequence LIMIT 50) UNION (SELECT l.sequence FROM unnest(ARRAY(SELECT name FROM bc_orders.event_types)) ty(name)
 CROSS JOIN LATERAL (SELECT DISTINCT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-3' AND tag_value='group-0')
 AND t.event_type_name=ty.name AND t.sequence>-1 AND t.sequence<=999999 ORDER BY t.sequence LIMIT 50) l
 ORDER BY l.sequence LIMIT 50) UNION (SELECT l.sequence FROM unnest(ARRAY(SELECT name FROM bc_orders.event_types)) ty(name)
 CROSS JOIN LATERAL (SELECT DISTINCT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-4' AND tag_value='group-0')
 AND t.event_type_name=ty.name AND t.sequence>-1 AND t.sequence<=999999 ORDER BY t.sequence LIMIT 50) l
 ORDER BY l.sequence LIMIT 50) UNION (SELECT l.sequence FROM unnest(ARRAY(SELECT name FROM bc_orders.event_types)) ty(name)
 CROSS JOIN LATERAL (SELECT DISTINCT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-5' AND tag_value='group-0')
 AND t.event_type_name=ty.name AND t.sequence>-1 AND t.sequence<=999999 ORDER BY t.sequence LIMIT 50) l
 ORDER BY l.sequence LIMIT 50) UNION (SELECT l.sequence FROM unnest(ARRAY(SELECT name FROM bc_orders.event_types)) ty(name)
 CROSS JOIN LATERAL (SELECT DISTINCT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-6' AND tag_value='group-0')
 AND t.event_type_name=ty.name AND t.sequence>-1 AND t.sequence<=999999 ORDER BY t.sequence LIMIT 50) l
 ORDER BY l.sequence LIMIT 50) UNION (SELECT l.sequence FROM unnest(ARRAY(SELECT name FROM bc_orders.event_types)) ty(name)
 CROSS JOIN LATERAL (SELECT DISTINCT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-7' AND tag_value='group-0')
 AND t.event_type_name=ty.name AND t.sequence>-1 AND t.sequence<=999999 ORDER BY t.sequence LIMIT 50) l
 ORDER BY l.sequence LIMIT 50) UNION (SELECT l.sequence FROM unnest(ARRAY(SELECT name FROM bc_orders.event_types)) ty(name)
 CROSS JOIN LATERAL (SELECT DISTINCT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-8' AND tag_value='group-0')
 AND t.event_type_name=ty.name AND t.sequence>-1 AND t.sequence<=999999 ORDER BY t.sequence LIMIT 50) l
 ORDER BY l.sequence LIMIT 50))) ORDER BY e.sequence LIMIT 50;
\echo wide_union_count
EXPLAIN (ANALYZE,BUFFERS) SELECT count(*) FROM bc_orders.events e JOIN bc_orders.event_types et ON et.name=e.event_type_name WHERE e.sequence>-1 AND e.sequence = ANY(ARRAY((SELECT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-1' AND tag_value='group-0')
 AND t.event_type_name=ANY(ARRAY(SELECT name FROM bc_orders.event_types)) AND t.sequence>-1) UNION (SELECT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-2' AND tag_value='group-0')
 AND t.event_type_name=ANY(ARRAY(SELECT name FROM bc_orders.event_types)) AND t.sequence>-1) UNION (SELECT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-3' AND tag_value='group-0')
 AND t.event_type_name=ANY(ARRAY(SELECT name FROM bc_orders.event_types)) AND t.sequence>-1) UNION (SELECT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-4' AND tag_value='group-0')
 AND t.event_type_name=ANY(ARRAY(SELECT name FROM bc_orders.event_types)) AND t.sequence>-1) UNION (SELECT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-5' AND tag_value='group-0')
 AND t.event_type_name=ANY(ARRAY(SELECT name FROM bc_orders.event_types)) AND t.sequence>-1) UNION (SELECT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-6' AND tag_value='group-0')
 AND t.event_type_name=ANY(ARRAY(SELECT name FROM bc_orders.event_types)) AND t.sequence>-1) UNION (SELECT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-7' AND tag_value='group-0')
 AND t.event_type_name=ANY(ARRAY(SELECT name FROM bc_orders.event_types)) AND t.sequence>-1) UNION (SELECT t.sequence FROM bc_orders.event_tag_refs t WHERE t.tag_id=(SELECT id FROM bc_orders.event_tag_values WHERE tag_key='tag-8' AND tag_value='group-0')
 AND t.event_type_name=ANY(ARRAY(SELECT name FROM bc_orders.event_types)) AND t.sequence>-1)));
