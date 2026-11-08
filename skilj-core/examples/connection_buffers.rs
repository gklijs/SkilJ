//! SQLx-only buffer experiment; no skilj runtime, cache, writes or schema setup.
//! See docs/benchmarks/connection-buffers/README.md for methodology and limits.
//! DATABASE_URL=postgres://... cargo run --release -p skilj-core \
//!   --example connection_buffers -- boundary 32 2097152 8 5

use futures_util::future::try_join_all;
use sqlx::{postgres::PgPoolOptions, Connection, PgConnection, Row};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

// Count requested Rust allocation sizes, not allocator arenas/usable sizes or RSS.
// SAFETY: every operation delegates to System with the original pointer/layout;
// bookkeeping uses allocation-free atomics and counts only successful allocations.
struct CountingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicUsize = AtomicUsize::new(0);
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size(), Relaxed);
            REQUESTED.fetch_add(layout.size(), Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size(), Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            // Subtract first so concurrently sampled live bytes cannot underflow.
            LIVE.fetch_sub(layout.size(), Relaxed);
            LIVE.fetch_add(size, Relaxed);
            REQUESTED.fetch_add(size, Relaxed);
        }
        result
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn snapshot(stage: &str, cycle: usize) -> Result<(), Box<dyn std::error::Error>> {
    let live_bytes = LIVE.load(Relaxed);
    let status = std::fs::read_to_string("/proc/self/status")?;
    let rss_kib: usize = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .ok_or("missing VmRSS (requires Linux)")?
        .split_whitespace()
        .nth(1)
        .ok_or("missing RSS value")?
        .parse()?;
    println!(
        "{}",
        serde_json::json!({"stage": stage, "cycle": cycle,
            "rss_kib": rss_kib, "live_requested_bytes": live_bytes})
    );
    Ok(())
}

async fn echo(connection: &mut PgConnection, payload: &str) -> Result<(), sqlx::Error> {
    // fetch_all drains the protocol; all row references die before shrinking.
    let rows = sqlx::query("SELECT $1::text")
        .bind(payload)
        .fetch_all(connection)
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<&str, _>(0)?, payload);
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let policy = arguments.first().map(String::as_str).unwrap_or("none");
    if !matches!(policy, "none" | "boundary" | "every-query") {
        return Err("policy must be none, boundary or every-query".into());
    }
    let argument = |index: usize, default: usize| -> Result<usize, std::num::ParseIntError> {
        arguments.get(index).map_or(Ok(default), |s| s.parse())
    };
    let count = u32::try_from(argument(1, 32)?)?;
    let bytes = argument(2, 2 * 1024 * 1024)?;
    let passes = argument(3, 8)?;
    let idle = Duration::from_secs(u64::try_from(argument(4, 5)?)?);
    if count == 0 || bytes == 0 || passes == 0 {
        return Err("connections, payload bytes and passes must be positive".into());
    }
    let pool = PgPoolOptions::new()
        .min_connections(count)
        .max_connections(count)
        .idle_timeout(None)
        .max_lifetime(None)
        .test_before_acquire(false)
        .after_connect(|_, _| {
            Box::pin(async {
                OPENED.fetch_add(1, Relaxed);
                Ok(())
            })
        })
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    static OPENED: AtomicUsize = AtomicUsize::new(0);
    let version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(&pool)
        .await?;
    // Hold exactly these sockets across both cycles: no replacement can account
    // for any measured change. This intentionally bypasses pool release hooks.
    let mut connections = Vec::new();
    let mut backend_pids = Vec::new();
    for _ in 0..count {
        let mut connection = pool.acquire().await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await?;
        connections.push(connection);
        backend_pids.push(pid);
    }
    let payload = "x".repeat(bytes);
    println!(
        "{}",
        serde_json::json!({"policy": policy, "connections": count,
            "payload_bytes": bytes, "passes": passes, "idle_seconds": idle.as_secs(),
            "postgres": version, "backend_pids": backend_pids})
    );
    snapshot("warm_pool", 0)?;
    for cycle in 1..=2 {
        let allocated_before = REQUESTED.load(Relaxed);
        let started = Instant::now();
        let samples = try_join_all(connections.iter_mut().map(|connection| {
            let payload = &payload;
            async move {
                let mut latencies = Vec::with_capacity(passes);
                for pass in 0..passes {
                    let query_started = Instant::now();
                    echo(connection, payload).await?;
                    if policy == "every-query" || (policy == "boundary" && pass + 1 == passes) {
                        connection.shrink_buffers();
                    }
                    latencies.push(query_started.elapsed().as_secs_f64() * 1000.0);
                }
                Ok::<_, sqlx::Error>(latencies)
            }
        }))
        .await?;
        let mut latencies: Vec<_> = samples.into_iter().flatten().collect();
        let elapsed = started.elapsed().as_secs_f64();
        let requested_bytes = REQUESTED.load(Relaxed) - allocated_before;
        latencies.sort_by(f64::total_cmp);
        let percentile = |n: usize| latencies[(latencies.len() * n).div_ceil(100) - 1];
        println!(
            "{}",
            serde_json::json!({"cycle": cycle, "successful_queries": latencies.len(),
                "queries_per_second": latencies.len() as f64 / elapsed,
                "p95_ms": percentile(95), "p99_ms": percentile(99),
                "requested_bytes_during_load": requested_bytes,
                "load_seconds_including_boundary": elapsed})
        );
        drop(latencies);
        snapshot("after_load_and_policy", cycle)?;
        // A BytesMut allocation can still be referenced by a protocol message;
        // record a completed subsequent read separately from shrink itself.
        for (connection, original_pid) in connections.iter_mut().zip(&backend_pids) {
            echo(connection, "small payload after large response").await?;
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **connection)
                .await?;
            assert_eq!(pid, *original_pid);
        }
        snapshot("after_small_queries", cycle)?;
        tokio::time::sleep(idle).await;
        snapshot("idle", cycle)?;
    }
    assert_eq!(OPENED.load(Relaxed), count as usize);
    println!(
        "{}",
        serde_json::json!({"connections_opened": OPENED.load(Relaxed),
        "replacements": 0, "cycles": 2})
    );
    drop(connections);
    pool.close().await;
    snapshot("pool_closed", 2)?;
    Ok(())
}
