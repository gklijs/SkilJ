//! Reproducible read probe. Run explicitly with DATABASE_URL and:
//! cargo test --release -p skilj-core --test authorized_event_bench -- --ignored --nocapture
//! SKILJ_BENCH_EVENTS defaults to one million; SKILJ_BENCH_RUNS to 20.
use chrono::{TimeZone, Utc};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::db::{self, AuthorizedEventQuery};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{self, BoundedContext, BoundedContextStatus, Event, EventType};
use skilj_core::shared::Tag;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

struct Counting;
static BYTES: AtomicUsize = AtomicUsize::new(0);
// Counts requested allocation bytes across the process, including SQLx/Tokio.
// Forward every allocation/deallocation unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        BYTES.fetch_add(layout.size(), Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        BYTES.fetch_add(size, Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn bounded_context(status: BoundedContextStatus) -> BoundedContext {
    BoundedContext {
        name: "orders".into(),
        status,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
        template: None,
    }
}

fn access_mapping(status: RoleStatus, level: AccessLevel) -> RoleAccessMapping {
    RoleAccessMapping {
        role: Role {
            id: "role-1".into(),
            external_subject: "admin@example.com".into(),
            name: "Admin".into(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: timestamp(0),
            revoked_at: None,
        },
        bounded_context: bounded_context(BoundedContextStatus::Active),
        level,
        can_read_sensitive: false,
        scope: None,
        capabilities: Default::default(),
        delegable: Default::default(),
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn event_type(name: &str) -> EventType {
    EventType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: name.into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        blob_fields: Vec::new(),
        external_creation_allowed: false,
        direct_creation_allowed: false,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: false,
    }
}

fn tag(key: &str, value: &str) -> Tag {
    Tag {
        key: key.into(),
        value: Some(value.into()),
    }
}

#[tokio::test]
#[ignore = "million-row read benchmark; run explicitly"]
async fn authorized_event_reads() {
    let reuse = std::env::var_os("SKILJ_BENCH_REUSE").is_some();
    let url = if reuse {
        std::env::var("DATABASE_URL")
            .expect("DATABASE_URL to the previously seeded benchmark database")
    } else {
        skilj_test_support::database_url("skilj_authorized_event_bench")
            .await
            .expect("Postgres")
    };
    let pool = db::connect(&url).await.unwrap();
    if !reuse {
        db::migrate(&pool).await.unwrap();
    }
    let bc = bounded_context(BoundedContextStatus::Active);
    if !reuse {
        db::insert_bounded_context(&pool, &bc).await.unwrap();
    }
    let mut owned = event_type("Owned");
    owned.owner_tag_key = Some("owner".into());
    let sparse = event_type("Sparse");
    for ty in [&owned, &sparse].into_iter().filter(|_| !reuse) {
        db::upsert_event_type(&pool, ty).await.unwrap();
    }
    let n: i64 = std::env::var("SKILJ_BENCH_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let runs: usize = std::env::var("SKILJ_BENCH_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20)
        .max(1);
    let started = Instant::now();
    if !reuse {
        sqlx::query(
        r#"INSERT INTO bc_orders.events (sequence,event_type_name,payload,metadata_type,metadata_version,metadata_client_id,metadata_created_at,metadata_correlation_id,tags,origin_kind)
         SELECT s, CASE WHEN s % 1000 = 0 THEN 'Sparse' ELSE 'Owned' END,
            '{"amount":10,"text":"' || repeat('x',256) || '"}', 'event',1,'bench',now(),
            'corr-' || (s % 1000),
            jsonb_build_array(jsonb_build_object('key','owner','value','tenant-' || (s % 1000))) ||
            (SELECT jsonb_agg(jsonb_build_object('key','tag-' || k,'value','group-' || (s % 2))) FROM generate_series(1,8) k),
            'directly_created' FROM generate_series(0,$1-1) s"#
    ).bind(n).execute(&pool).await.unwrap();
    }
    for table in ["events", "event_tag_refs", "event_tag_values"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "VACUUM ANALYZE bc_orders.{table}"
        )))
        .execute(&pool)
        .await
        .unwrap();
    }
    println!(
        "seed events={n} seconds={:.2}",
        started.elapsed().as_secs_f64()
    );
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let broad_tags = vec![tag("tag-1", "group-0")];
    let wide_tags: Vec<_> = (1..=8)
        .map(|k| tag(&format!("tag-{k}"), "group-0"))
        .collect();
    let scenarios = [
        ("all", vec![], None, None, None, None),
        ("sparse-type", vec![sparse], None, None, None, None),
        (
            "sparse-owner",
            vec![owned],
            None,
            Some("tenant-1"),
            None,
            None,
        ),
        (
            "sparse-correlation",
            vec![],
            None,
            None,
            Some("corr-1"),
            None,
        ),
        ("broad-tag", vec![], Some(broad_tags), None, None, None),
        ("wide-union", vec![], Some(wide_tags), None, None, None),
        ("recent-cache", vec![], None, None, None, Some(n - 501)),
    ];
    for (name, types, tags, scope, correlation, after) in scenarios {
        let mut mapping = mapping.clone();
        mapping.scope = scope.map(str::to_owned);
        let query = AuthorizedEventQuery::for_admin(
            &mapping,
            &types,
            tags.as_deref(),
            after,
            None,
            correlation,
        )
        .unwrap();
        for count in [false, true] {
            if count && after.is_some() {
                continue;
            }
            let mut expected = None;
            let only = std::env::var("SKILJ_BENCH_ONLY").ok();
            for implementation in ["old-cold", "old-warm", "sql"]
                .into_iter()
                .filter(|implementation| only.as_deref().is_none_or(|only| only == *implementation))
            {
                // Baseline counts materialize one million rows: one sample each;
                // new path/page baselines get enough samples for empirical tails.
                let samples = if count && implementation != "sql" {
                    1
                } else {
                    runs
                };
                let cache = EventCache::new(1000);
                let mut timings = vec![];
                let mut allocations = vec![];
                let mut result = 0;
                for _ in 0..samples {
                    let cold = EventCache::new(1000);
                    let cache = if implementation == "old-cold" {
                        &cold
                    } else {
                        &cache
                    };
                    BYTES.store(0, Relaxed);
                    let started = Instant::now();
                    if implementation == "sql" {
                        result = if count {
                            db::count_authorized_events(&pool, &query).await.unwrap() as usize
                        } else {
                            db::collect_authorized_event_page(&pool, &query, 50, |rows, left| {
                                event_store::query_events_select(
                                    &mapping,
                                    &types,
                                    tags.as_deref(),
                                    after,
                                    correlation,
                                    rows,
                                    left,
                                )
                            })
                            .await
                            .unwrap()
                            .events
                            .len()
                        };
                    } else if count {
                        let mut total = 0;
                        let mut sum = |rows: &[Event]| {
                            total += event_store::count_events(
                                &mapping,
                                &types,
                                tags.as_deref(),
                                correlation,
                                rows,
                            )?;
                            Ok(true)
                        };
                        if let Some(tags) = &tags {
                            db::for_each_tagged_event_chunk(
                                &pool, &bc.name, tags, -1, 1000, &mut sum,
                            )
                            .await
                            .unwrap();
                        } else {
                            db::for_each_event_chunk(
                                &pool, cache, &bc.name, None, -1, 1000, &mut sum,
                            )
                            .await
                            .unwrap();
                        }
                        result = total as usize;
                    } else {
                        let select = |rows: &[Event], left| {
                            event_store::query_events_select(
                                &mapping,
                                &types,
                                tags.as_deref(),
                                after,
                                correlation,
                                rows,
                                left,
                            )
                        };
                        result = if let Some(tags) = &tags {
                            db::collect_tagged_event_page(
                                &pool,
                                &bc.name,
                                tags,
                                after.unwrap_or(-1),
                                None,
                                50,
                                select,
                            )
                            .await
                            .unwrap()
                            .len()
                        } else {
                            db::collect_event_page(
                                &pool,
                                cache,
                                &bc.name,
                                None,
                                after.unwrap_or(-1),
                                None,
                                50,
                                select,
                            )
                            .await
                            .unwrap()
                            .len()
                        };
                    }
                    if let Some(expected) = expected {
                        assert_eq!(result, expected, "{name} {implementation}");
                    } else {
                        expected = Some(result);
                    }
                    timings.push(started.elapsed().as_secs_f64() * 1000.0);
                    allocations.push(BYTES.load(Relaxed));
                }
                timings.sort_by(f64::total_cmp);
                allocations.sort();
                let percentile = |p: usize| {
                    timings[((timings.len() * p).div_ceil(100) - 1).min(timings.len() - 1)]
                };
                println!("{name} {} {implementation} samples={samples} result={result} median_ms={:.3} p95_ms={:.3} p99_ms={:.3} median_requested_bytes={}",if count {"count"}else{"page"},percentile(50),percentile(95),percentile(99),allocations[allocations.len()/2]);
            }
        }
    }
    println!(
        "{}",
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .filter(|s| s.starts_with("VmHWM:") || s.starts_with("VmRSS:"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    pool.close().await;
}
