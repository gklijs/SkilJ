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
//!   visible in a comparison, never to model anything real. It lands
//!   before skilj handles a request, so a scenario bound by the bounded
//!   context's lock absorbs it; `command_throughput`'s
//!   `SKILJ_BENCH_LATENCY_MS` delays Postgres round trips instead.
//!
//! [`FORMAT`] is the layout of a record. Each benchmark carries a version
//! of its own as well, which it raises when what a scenario measures
//! changes; the comparison refuses to compare records of different
//! versions.
//!
//! The lines are written by hand rather than through `serde_json`: a new
//! dependency here would change the workspace lock that the CGA panic
//! analysis pins (`scripts/cga/workspace.lock`). The benchmarks build
//! their settings with `serde_json` themselves and pass the JSON text.

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
    /// projections, ...), as the text of a JSON object.
    pub settings: String,
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
        let samples: Vec<String> = scenario
            .samples
            .iter()
            .map(|sample| micros(sample).to_string())
            .collect();
        self.write(format!(
            "{{\"format\":{FORMAT},\"kind\":\"scenario\",\"benchmark\":{},\"benchmark_version\":{},\
             \"scenario\":{},\"round\":{},\"unit\":{},\"operations\":{},\"expected_operations\":{},\
             \"elapsed_us\":{},\"samples_us\":[{}],\"settings\":{},\"slowdown_us\":{},\"peak_rss_kb\":{}}}",
            json_string(self.benchmark),
            self.version,
            json_string(&scenario.name),
            scenario.round,
            json_string(scenario.unit),
            scenario.operations,
            scenario.expected_operations,
            micros(&scenario.elapsed),
            samples.join(","),
            scenario.settings,
            json_number(slowdown_duration().map(|d| micros(&d))),
            json_number(peak_rss_kb()),
        ));
    }

    /// Records that the benchmark finished, with `environment`, the text
    /// of a JSON object: what it knows about where it ran (Postgres
    /// settings, for one). The comparison treats a run without this line
    /// as failed.
    pub fn complete(&self, environment: String) {
        if !self.is_recording() {
            return;
        }
        self.write(format!(
            "{{\"format\":{FORMAT},\"kind\":\"complete\",\"benchmark\":{},\"benchmark_version\":{},\
             \"scenarios\":{},\"environment\":{environment},\"peak_rss_kb\":{}}}",
            json_string(self.benchmark),
            self.version,
            self.scenarios.load(Ordering::Relaxed),
            json_number(peak_rss_kb()),
        ));
    }

    fn write(&self, mut line: String) {
        let Some(file) = &self.file else { return };
        let mut file = file.lock().unwrap();
        line.push('\n');
        file.write_all(line.as_bytes())
            .and_then(|()| file.flush())
            .expect("writing a benchmark record");
    }
}

/// `value` as a JSON string literal.
pub fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_number(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_string(), |n| n.to_string())
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

/// The server settings a measurement depends on, as the text of a JSON
/// object for a [`Recorder::complete`] environment: the version and the
/// durability settings, which an embedded or tuned-down server changes.
pub async fn postgres_settings(pool: &sqlx::PgPool) -> String {
    let settings: Vec<(String, String)> = sqlx::query_as(
        "SELECT name, setting FROM pg_settings WHERE name IN \
         ('fsync', 'synchronous_commit', 'full_page_writes', 'server_version', \
          'shared_buffers', 'max_connections', 'wal_level') ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .expect("reading pg_settings");
    let fields: Vec<String> = settings
        .iter()
        .map(|(name, value)| format!("{}:{}", json_string(name), json_string(value)))
        .collect();
    format!("{{{}}}", fields.join(","))
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
            settings: r#"{"workers":1}"#.to_string(),
        });
        recorder.complete(r#"{"postgres":{}}"#.to_string());
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(
            lines[0].starts_with(
                r#"{"format":1,"kind":"scenario","benchmark":"example","benchmark_version":3,"scenario":"one/1w","round":1,"unit":"command","operations":2,"expected_operations":2,"elapsed_us":3000,"samples_us":[1500,1400],"settings":{"workers":1},"slowdown_us":null,"peak_rss_kb":"#
            ),
            "{}",
            lines[0]
        );
        assert!(
            lines[1].starts_with(
                r#"{"format":1,"kind":"complete","benchmark":"example","benchmark_version":3,"scenarios":1,"environment":{"postgres":{}},"peak_rss_kb":"#
            ),
            "{}",
            lines[1]
        );
    }

    #[test]
    fn escapes_json_strings() {
        assert_eq!(json_string(r#"a"b\c"#), r#""a\"b\\c""#);
        assert_eq!(json_string("tab\there"), r#""tab\u0009here""#);
        assert_eq!(json_string("16.15 (Ubuntu)"), r#""16.15 (Ubuntu)""#);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn reads_this_process_peak_memory() {
        assert!(peak_rss_kb().unwrap() > 0);
    }
}
