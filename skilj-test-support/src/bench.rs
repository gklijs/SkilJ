//! Machine-readable records for the `#[ignore]`d benchmarks
//! (`command_throughput`, `inbound_throughput`,
//! `projection_catch_up_throughput`), which `scripts/bench/compare.mjs`
//! collects to compare two commits (docs/performance.md, "Comparing two
//! commits").
//!
//! Nothing here changes a benchmark unless the environment asks for it:
//!
//! - `SKILJ_BENCH_RECORD=<file>` appends one JSON line per measured
//!   scenario, and one `complete` line once the benchmark finished, to
//!   `<file>`. A recorded benchmark cannot skip: [`Recorder::skip`]
//!   panics instead, so a missing database fails the run rather than
//!   producing an empty one.
//! - `SKILJ_BENCH_ROUNDS=<n>` sets how many measured rounds a benchmark
//!   runs ([`rounds`]).
//! - `SKILJ_BENCH_SLOWDOWN_US=<n>` adds a fixed delay of at least `n`
//!   microseconds inside every measured operation ([`slowdown`],
//!   [`slowdown_blocking`]). It exists to show that a known slowdown is
//!   visible in a comparison, never to model anything real.
//!
//! [`FORMAT`] is the layout of a record. Each benchmark carries a version
//! of its own as well, which it raises when what a scenario measures
//! changes; the comparison refuses to compare records of different
//! versions.

use std::io::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// The layout of a record line. Raised when a field changes meaning or
/// goes away; adding a field does not raise it.
pub const FORMAT: u32 = 1;

/// One measured scenario, as [`Recorder::scenario`] writes it.
pub struct Scenario<'a> {
    /// Stable identity of the scenario within its benchmark, e.g.
    /// `hot/8w` - what the comparison groups by.
    pub name: String,
    /// The measured round it belongs to, from 1.
    pub round: usize,
    /// What one operation is: `command`, `message`, `event`.
    pub unit: &'static str,
    /// Operations the scenario completed.
    pub operations: usize,
    /// Operations it was meant to complete; the comparison fails a
    /// record whose count differs.
    pub expected_operations: usize,
    /// Wall-clock time of the whole scenario.
    pub elapsed: Duration,
    /// Raw per-operation (or per-call) latencies, in any order. Empty
    /// when the scenario has no per-operation latency.
    pub samples: &'a [Duration],
    /// The settings the scenario ran with (workers, batch, partitions,
    /// projections, ...).
    pub settings: serde_json::Value,
}

/// Writes one benchmark's records to `SKILJ_BENCH_RECORD`, when set.
pub struct Recorder {
    benchmark: &'static str,
    version: u32,
    file: Option<Mutex<std::fs::File>>,
    scenarios: AtomicUsize,
}

impl Recorder {
    /// A recorder for `benchmark` at `version`, writing to
    /// `SKILJ_BENCH_RECORD` if that is set and to nowhere otherwise.
    pub fn from_env(benchmark: &'static str, version: u32) -> Self {
        let file = std::env::var_os("SKILJ_BENCH_RECORD").map(|path| {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap_or_else(|e| panic!("SKILJ_BENCH_RECORD={path:?}: {e}"));
            Mutex::new(file)
        });
        Self {
            benchmark,
            version,
            file,
            scenarios: AtomicUsize::new(0),
        }
    }

    /// Whether records are being written.
    pub fn is_recording(&self) -> bool {
        self.file.is_some()
    }

    /// Reports that the benchmark cannot run: prints the `skipping:`
    /// line the test suites use, or panics when recording, since a
    /// recorded run that skipped would compare as nothing at all.
    pub fn skip(&self, reason: &str) {
        assert!(
            !self.is_recording(),
            "{}: a recorded benchmark cannot skip: {reason}",
            self.benchmark
        );
        eprintln!("skipping: {reason}");
    }

    /// Records one measured scenario.
    pub fn scenario(&self, scenario: Scenario<'_>) {
        if !self.is_recording() {
            return;
        }
        self.scenarios.fetch_add(1, Ordering::Relaxed);
        let samples: Vec<u64> = scenario.samples.iter().map(micros).collect();
        self.write(serde_json::json!({
            "format": FORMAT,
            "kind": "scenario",
            "benchmark": self.benchmark,
            "benchmark_version": self.version,
            "scenario": scenario.name,
            "round": scenario.round,
            "unit": scenario.unit,
            "operations": scenario.operations,
            "expected_operations": scenario.expected_operations,
            "elapsed_us": micros(&scenario.elapsed),
            "samples_us": samples,
            "settings": scenario.settings,
            "slowdown_us": slowdown_duration().map(|d| micros(&d)),
            "peak_rss_kb": peak_rss_kb(),
        }));
    }

    /// Records that the benchmark finished, with `environment`: what it
    /// knows about where it ran (Postgres settings, for one). The
    /// comparison treats a run without this line as failed.
    pub fn complete(&self, environment: serde_json::Value) {
        if !self.is_recording() {
            return;
        }
        self.write(serde_json::json!({
            "format": FORMAT,
            "kind": "complete",
            "benchmark": self.benchmark,
            "benchmark_version": self.version,
            "scenarios": self.scenarios.load(Ordering::Relaxed),
            "environment": environment,
            "peak_rss_kb": peak_rss_kb(),
        }));
    }

    fn write(&self, record: serde_json::Value) {
        let Some(file) = &self.file else { return };
        let mut file = file.lock().unwrap();
        let mut line = record.to_string();
        line.push('\n');
        file.write_all(line.as_bytes())
            .and_then(|()| file.flush())
            .expect("writing a benchmark record");
    }
}

fn micros(duration: &Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// `SKILJ_BENCH_ROUNDS`, or `default`.
pub fn rounds(default: usize) -> usize {
    match std::env::var("SKILJ_BENCH_ROUNDS") {
        Ok(value) => value
            .parse()
            .ok()
            .filter(|&n| n > 0)
            .unwrap_or_else(|| panic!("SKILJ_BENCH_ROUNDS={value:?}")),
        Err(_) => default,
    }
}

fn slowdown_duration() -> Option<Duration> {
    static SLOWDOWN: OnceLock<Option<Duration>> = OnceLock::new();
    *SLOWDOWN.get_or_init(|| {
        let value = std::env::var("SKILJ_BENCH_SLOWDOWN_US").ok()?;
        let us: u64 = value
            .parse()
            .unwrap_or_else(|_| panic!("SKILJ_BENCH_SLOWDOWN_US={value:?}"));
        (us > 0).then(|| Duration::from_micros(us))
    })
}

/// Waits `SKILJ_BENCH_SLOWDOWN_US`, if set; returns at once otherwise.
/// Call it inside a measured operation. Tokio's timer rounds the wait up
/// to its millisecond resolution.
pub async fn slowdown() {
    if let Some(delay) = slowdown_duration() {
        tokio::time::sleep(delay).await;
    }
}

/// [`slowdown`] for code that cannot await: blocks the calling thread.
pub fn slowdown_blocking() {
    if let Some(delay) = slowdown_duration() {
        std::thread::sleep(delay);
    }
}

/// The server settings a measurement depends on, as a JSON object for a
/// [`Recorder::complete`] environment: the version and the durability
/// settings, which an embedded or tuned-down server changes.
pub async fn postgres_settings(pool: &sqlx::PgPool) -> serde_json::Value {
    let settings: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, setting FROM pg_settings WHERE name IN \
         ('fsync', 'synchronous_commit', 'full_page_writes', 'server_version', \
          'shared_buffers', 'max_connections', 'wal_level') ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .expect("reading pg_settings");
    settings
        .into_iter()
        .map(|(name, value)| (name, serde_json::Value::String(value)))
        .collect::<serde_json::Map<_, _>>()
        .into()
}

/// The benchmark process's own peak resident memory so far (`VmHWM`), in
/// KiB - not the database's, which runs in its own process. [`None`]
/// where `/proc/self/status` doesn't exist (anything but Linux).
pub fn peak_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_scenarios_and_completion_as_json_lines() {
        let path = std::env::temp_dir().join(format!("skilj-bench-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let recorder = Recorder {
            benchmark: "example",
            version: 3,
            file: Some(Mutex::new(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .unwrap(),
            )),
            scenarios: AtomicUsize::new(0),
        };
        recorder.scenario(Scenario {
            name: "one/1w".to_string(),
            round: 1,
            unit: "command",
            operations: 2,
            expected_operations: 2,
            elapsed: Duration::from_millis(3),
            samples: &[Duration::from_micros(1500), Duration::from_micros(1400)],
            settings: serde_json::json!({ "workers": 1 }),
        });
        recorder.complete(serde_json::json!({}));
        let lines: Vec<serde_json::Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["kind"], "scenario");
        assert_eq!(lines[0]["benchmark_version"], 3);
        assert_eq!(lines[0]["samples_us"], serde_json::json!([1500, 1400]));
        assert_eq!(lines[0]["elapsed_us"], 3000);
        assert_eq!(lines[1]["kind"], "complete");
        assert_eq!(lines[1]["scenarios"], 1);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn reads_this_process_peak_memory() {
        assert!(peak_rss_kb().unwrap() > 0);
    }
}
