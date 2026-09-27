//! Postgres provisioning for this workspace's own integration tests -
//! a dev-dependency only, never published.
//!
//! Every Postgres-backed test file used to start its own
//! `postgresql_embedded` server and keep it in a `static`. Rust never
//! drops statics, so the server was never stopped and its temp data dir
//! never removed: every `cargo test --workspace` left roughly one live
//! Postgres per test binary behind, plus a data dir on `/tmp`. On a
//! RAM-backed `/tmp` (WSL) a few runs filled it outright
//! (docs/architecture.md §66). [`database_url`] owns the server instead
//! and hands it to a watchdog process that cleans up once the test
//! process is gone, however it exits. The bridge crates' broker
//! containers had the same leak; [`remove_container_on_exit`] gives them
//! the same watchdog.

use postgresql_embedded::PostgreSQL;
use tokio::sync::{Mutex, OnceCell};

static EMBEDDED: OnceCell<Option<Mutex<PostgreSQL>>> = OnceCell::const_new();

/// A URL for a Postgres database named `database_name`: `DATABASE_URL`
/// as-is when it's set (reachability is the caller's to check, as
/// before), otherwise [`embedded_database_url`].
pub async fn database_url(database_name: &str) -> Option<String> {
    match std::env::var("DATABASE_URL") {
        Ok(database_url) => Some(database_url),
        Err(_) => embedded_database_url(database_name).await,
    }
}

/// `database_name` on this process's one embedded server - started on
/// first use, the database created if it doesn't exist yet - ignoring
/// `DATABASE_URL`. For a test that needs a database no earlier run has
/// touched. `None`, after a `skipping: ...` line on stderr (which CI
/// greps for), when the server can't be set up.
pub async fn embedded_database_url(database_name: &str) -> Option<String> {
    let server = EMBEDDED.get_or_init(start).await.as_ref()?.lock().await;
    match server.database_exists(database_name).await {
        Ok(true) => {}
        Ok(false) => {
            if let Err(e) = server.create_database(database_name).await {
                eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
                return None;
            }
        }
        Err(e) => {
            eprintln!("skipping: embedded PostgreSQL database_exists failed: {e}");
            return None;
        }
    }
    Some(server.settings().url(database_name))
}

async fn start() -> Option<Mutex<PostgreSQL>> {
    let mut server = PostgreSQL::default();
    if let Err(e) = server.setup().await {
        eprintln!(
            "skipping: DATABASE_URL not set and embedded PostgreSQL setup failed \
             (no network egress to fetch the binary, or a missing system library \
             like libxml2 it links against): {e}"
        );
        return None;
    }
    // Before `start()`, so a data dir `setup()` already created is
    // cleaned up even if starting fails.
    spawn_cleanup_watchdog(&server);
    if let Err(e) = server.start().await {
        eprintln!("skipping: embedded PostgreSQL failed to start: {e}");
        return None;
    }
    Some(Mutex::new(server))
}

/// Removes the Docker container `container_id` (a testcontainers
/// `ContainerAsync::id()`) once this process is gone. testcontainers
/// removes a container when its handle drops, but a broker shared across
/// a test binary lives in a `static`, which never drops - so without this
/// every run left its Kafka/Artemis/NATS container running. Uses the
/// same `docker` CLI and environment (`DOCKER_HOST` included) the tests
/// themselves run under. Best effort, like the Postgres watchdog.
pub fn remove_container_on_exit(container_id: &str) {
    spawn_watchdog(
        "container",
        r#"docker rm -f -v "$2""#,
        &[std::ffi::OsStr::new(container_id)],
    );
}

/// Stops `server` and deletes its data dir and password file once this
/// process is gone.
fn spawn_cleanup_watchdog(server: &PostgreSQL) {
    let settings = server.settings();
    // `postgresql_embedded` puts the password file alone in a fresh
    // `tempfile` dir, which its own `Drop` leaves behind - but falls back
    // to the current directory if that dir can't be created. Only a real
    // `.tmp*` dir under the temp dir is removed whole; otherwise just the
    // file, never whatever directory the tests happen to run in.
    let password_path = match settings.password_file.parent() {
        Some(dir)
            if dir.starts_with(std::env::temp_dir())
                && dir
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(".tmp")) =>
        {
            dir.to_path_buf()
        }
        _ => settings.password_file.clone(),
    };
    let pg_ctl = settings.binary_dir().join("pg_ctl");
    spawn_watchdog(
        "embedded PostgreSQL",
        r#""$2" stop -D "$3" -m immediate -w
           rm -rf "$3"
           rm -rf "$4""#,
        &[
            pg_ctl.as_os_str(),
            settings.data_dir.as_os_str(),
            password_path.as_os_str(),
        ],
    );
}

/// A `sh` child that polls until this process is gone, then runs
/// `cleanup` with `args` as `$2`, `$3`, ... (`$1` is this process's pid).
/// A separate process rather than an exit hook, because the exits that
/// matter most here (Ctrl-C, a killed test binary, a crashed VM session)
/// run no exit hooks. Its own process group, so a terminal's Ctrl-C
/// doesn't take it down along with the tests. Best effort: if it can't
/// be spawned, the resource leaks exactly as it did before this crate
/// existed.
#[cfg(unix)]
fn spawn_watchdog(what: &str, cleanup: &str, args: &[&std::ffi::OsStr]) {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    let spawned = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "while kill -0 \"$1\" 2>/dev/null; do sleep 1; done\n{cleanup}"
        ))
        .arg("skilj-test-watchdog")
        .arg(std::process::id().to_string())
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn();
    if let Err(e) = spawned {
        eprintln!("warning: couldn't spawn the {what} cleanup watchdog: {e}");
    }
}

#[cfg(not(unix))]
fn spawn_watchdog(_what: &str, _cleanup: &str, _args: &[&std::ffi::OsStr]) {}
