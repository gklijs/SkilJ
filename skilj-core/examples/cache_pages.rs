//! Isolated allocation probe, not an application throughput benchmark.
//! Build/run instructions and limitations are in the accompanying review.
use skilj_core::bootstrap::ContextCreator;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::shared::{Metadata, Tag};
use skilj_core::{db, event_cache::EventCache};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

struct CountAlloc;
static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        CALLS.fetch_add(1, Relaxed);
        BYTES.fetch_add(layout.size(), Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        CALLS.fetch_add(1, Relaxed);
        BYTES.fetch_add(size, Relaxed);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOC: CountAlloc = CountAlloc;

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());
}

async fn run() {
    let pool =
        db::connect(&std::env::var("DATABASE_URL").expect("dedicated benchmark database URL"))
            .await
            .unwrap();
    db::migrate(&pool).await.unwrap();
    let now = "2026-11-08T00:00:00Z".parse().unwrap();
    let bc = BoundedContext {
        name: format!("cache_probe_{}", skilj_core::shared::generate_token_id()),
        status: BoundedContextStatus::Active,
        created_at: now,
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    let event_type = EventType {
        bounded_context: bc.clone(),
        name: "Deposited".into(),
        schema: format!(r#"{{"description":"{}"}}"#, "x".repeat(2048)),
        schema_version: 1,
        tag_mappings: vec![],
        owner_tag_key: None,
        sensitive_fields: vec![],
        private_fields: vec![],
        blob_fields: vec![],
        external_creation_allowed: true,
        direct_creation_allowed: true,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: true,
    };
    db::insert_bounded_context(&pool, &bc).await.unwrap();
    db::upsert_event_type(&pool, &event_type).await.unwrap();
    let rare = EventType {
        name: "Rare".into(),
        ..event_type.clone()
    };
    db::upsert_event_type(&pool, &rare).await.unwrap();
    let events: Vec<Event> = (0..10_000)
        .map(|sequence| Event {
            bounded_context: bc.clone(),
            event_type: if sequence % 100 == 0 {
                rare.clone()
            } else {
                event_type.clone()
            },
            payload: format!(r#"{{"note":"{}"}}"#, "y".repeat(1024)),
            metadata: Metadata {
                r#type: "Deposited".into(),
                version: 1,
                client_id: "client".into(),
                created_at: now,
                correlation_id: None,
                causation_id: None,
            },
            sequence,
            tags: vec![Tag {
                key: "account".into(),
                value: Some("A".into()),
            }],
            encryption_keys: vec![],
            origin: EventOrigin::DirectlyCreated,
        })
        .collect();
    // Insert through the normal writer so cache hydration/registrations are real.
    for event in &events {
        let sequence = db::next_sequence(&pool, &bc.name).await.unwrap();
        assert_eq!(sequence, event.sequence);
        db::insert_event(&pool, event, None).await.unwrap();
    }
    println!("window,page,selection,path,mean_requested_bytes,mean_allocations,p95_us");
    for window in [1000, 10_000] {
        let cache = EventCache::new(window);
        cache.warm(&pool, &bc.name).await.unwrap();
        let after = 10_000 - window as i64 - 1;
        for size in [1, 100] {
            for name in [None, Some("Rare")] {
                for bounded in [false, true] {
                    let mut times = Vec::new();
                    let mut bytes = 0;
                    let mut calls = 0;
                    for sample in 0..105 {
                        CALLS.store(0, Relaxed);
                        BYTES.store(0, Relaxed);
                        let start = Instant::now();
                        if bounded {
                            db::for_each_event_chunk(
                                &pool,
                                &cache,
                                &bc.name,
                                name,
                                after,
                                size,
                                |chunk| {
                                    black_box(chunk);
                                    Ok(false)
                                },
                            )
                            .await
                            .unwrap();
                        } else {
                            // Previous production selection order, including freshening
                            // and current registration copies on every served record.
                            let events = cache
                                .try_events_after(&pool, &bc.name, after)
                                .await
                                .unwrap()
                                .unwrap();
                            black_box(
                                events
                                    .into_iter()
                                    .filter(|e| name.is_none_or(|n| e.event_type.name == n))
                                    .take(size)
                                    .collect::<Vec<_>>(),
                            );
                        }
                        let elapsed = start.elapsed();
                        let allocated = BYTES.load(Relaxed);
                        let allocations = CALLS.load(Relaxed);
                        if sample >= 5 {
                            times.push(elapsed.as_nanos());
                            bytes += allocated;
                            calls += allocations;
                        }
                    }
                    times.sort_unstable();
                    println!(
                        "{window},{size},{},{},{},{},{:.1}",
                        name.unwrap_or("all"),
                        if bounded { "bounded" } else { "previous" },
                        bytes / 100,
                        calls / 100,
                        times[94] as f64 / 1000.0
                    );
                }
            }
        }
    }
    db::hard_delete_bounded_context(&pool, &bc.name)
        .await
        .unwrap();
}
