//! Isolated allocation probe, not an application throughput benchmark.
//! Build/run instructions and limitations are in the accompanying review.
use skilj_core::bootstrap::ContextCreator;
use skilj_core::event_store::{
    select_matching_events, BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::shared::{Metadata, QueryItem, Tag};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

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

fn measure<T>(name: &str, f: impl FnOnce() -> T) {
    CALLS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    let value = black_box(f());
    let calls = CALLS.load(Relaxed);
    let bytes = BYTES.load(Relaxed);
    println!("{name}: allocations={calls}, requested_bytes={bytes}");
    drop(value);
}

fn main() {
    let now = "2026-11-08T00:00:00Z".parse().unwrap();
    let bc = BoundedContext {
        name: "banking".into(),
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
    let events: Vec<Event> = (0..1000)
        .map(|sequence| Event {
            bounded_context: bc.clone(),
            event_type: event_type.clone(),
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
    let query = vec![QueryItem {
        event_types: vec!["Deposited".into()],
        tags: vec![],
        latest: None,
        snapshot: None,
    }];
    measure("select_matching_events, all 1000", || {
        select_matching_events(&query, &events)
    });
    let matching = select_matching_events(&query, &events);
    measure("queue copies, two histories", || {
        (events.clone(), matching.clone())
    });
    measure("cache clone then take 100", || {
        events.clone().into_iter().take(100).collect::<Vec<_>>()
    });
    measure("cache take 100 then clone", || {
        events.iter().take(100).cloned().collect::<Vec<_>>()
    });
    let shared: Vec<Arc<Event>> = events.iter().cloned().map(Arc::new).collect();
    measure("Arc handles, 1000", || shared.clone());
    measure("queue ownership transfer", || (events, matching));
}
