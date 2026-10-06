//! Postgres provisioning for this workspace's own integration tests -
//! a dev-dependency only, never published.
//!
//! One way in: [`database_url`] hands back a database of the calling
//! test binary's own, dropped and recreated on every call, so the tests
//! in that binary start from nothing whatever any earlier run left
//! behind. It prefers `DATABASE_URL`'s server when there is one, which
//! is how CI runs them (docs/architecture.md §166), and falls back to an
//! embedded server otherwise.
//!
//! Per-binary rather than workspace-wide matters because `cargo test
//! --workspace` runs every test binary against one `DATABASE_URL`. The
//! URL in it supplies the *server* - host, port, credentials, TLS - and
//! its own database name is ignored: sharing one database across
//! binaries is what broke them. Each binary's bounded contexts, roles,
//! superadmins, metrics and subscription rows piled into the same
//! schemas, and the synchronous schema work one binary's tests did
//! pushed the event dispatch loop in another's past its notification
//! timeout (see `skilj/tests/cross_instance.rs`). One database per
//! binary makes those collisions structurally impossible, and costs one
//! `CREATE DATABASE` per binary against a server that already had to be
//! running.
//!
//! Within a binary the database is still shared: tests in one file are
//! written to stay inside their own per-bounded-context schemas, and
//! reach the database through one `OnceCell`, so the drop happens once,
//! before the first test connects. A test that drops schemas, renames
//! tables, or needs `admin` not to exist yet wants a database to itself -
//! which is now just a second [`database_url`] call with its own name.
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

/// A URL for a Postgres database named `database_name`, emptied first:
/// on the server `DATABASE_URL` names, dropped and recreated there, or
/// on this process's one embedded server, or [`None`] if neither works.
///
/// The drop matters for more than tidiness. It is what makes a re-run
/// against a developer's own long-lived `DATABASE_URL` server behave
/// like CI's fresh service container, rather than inheriting the last
/// run's bounded-context schemas and fixed external subjects - several
/// tests assert on rows and schemas that must not exist yet, and would
/// otherwise fail on the second `cargo test` and nowhere else. The name
/// is deterministic rather than random for the same reason: a re-run
/// replaces the previous run's database instead of leaving one behind
/// per run.
///
/// `DATABASE_URL`'s role needs `CREATEDB`, and - for a database a
/// previous run may still have sessions attached to - permission to
/// terminate them, which is the same superuser privilege CI's
/// `postgres` role has.
///
/// `None`, after a `skipping: ...` line on stderr (which CI greps for),
/// when there's no usable server or the database can't be made.
pub async fn database_url(database_name: &str) -> Option<String> {
    let Some(database_url) = std::env::var("DATABASE_URL").ok() else {
        return embedded_database_url(database_name).await;
    };
    match fresh_named_database(&database_url, database_name).await {
        Ok(url) => Some(url),
        Err(e) => {
            eprintln!("skipping: provisioning the {database_name} database failed: {e}");
            None
        }
    }
}

/// A URL for an empty database named `database_name` on the server
/// `server_url` addresses. `Err` carries the reason to print after
/// `skipping:`.
///
/// The connect and both statements run against `server_url`'s own
/// database, which needs only to exist for `DROP`/`CREATE DATABASE` to
/// run in it.
async fn fresh_named_database(server_url: &str, database_name: &str) -> Result<String, String> {
    use sqlx::ConnectOptions as _;

    if !is_plain_identifier(database_name) {
        return Err(format!(
            "the database name {database_name:?} is not a plain identifier"
        ));
    }
    let options: sqlx::postgres::PgConnectOptions = server_url
        .parse()
        .map_err(|e| format!("parsing the database URL failed: {e}"))?;

    let admin = sqlx::PgPool::connect(server_url)
        .await
        .map_err(|e| format!("connecting to the server failed: {e}"))?;

    // `DROP DATABASE` refuses to run while anyone is connected, and a
    // previous run that was killed rather than exited leaves its
    // sessions behind - which is the one case where a re-run would
    // otherwise find a database it can't drop. Best effort: if this
    // role can't terminate other backends the `DROP` still succeeds on
    // a database nobody is attached to.
    let _ = sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(database_name)
    .execute(&admin)
    .await;
    for statement in [
        format!("DROP DATABASE IF EXISTS {database_name}"),
        format!("CREATE DATABASE {database_name}"),
    ] {
        run_as_admin(&admin, &statement).await?;
    }
    drop(admin);

    // `to_url_lossy` rebuilds the URL from the parsed options with the
    // database replaced, which keeps the server's host, port,
    // credentials and TLS settings rather than hand-assembling a second
    // copy of them and getting one of them wrong.
    Ok(options.database(database_name).to_url_lossy().to_string())
}

/// Runs `statement` against the admin pool, tolerating a lost race to
/// create a database another process just created - two `cargo test`
/// runs against one server can reach the same `CREATE DATABASE` at the
/// same time, and the loser is no worse off than before it ran.
///
/// `CREATE DATABASE` and `DROP DATABASE` cannot run inside a
/// transaction, and neither takes a bind parameter for the identifier -
/// so the name goes into the statement text. That is safe because every
/// caller passes a `&str` literal, and `is_plain_identifier` rejects
/// anything that isn't one.
async fn run_as_admin(admin: &sqlx::PgPool, statement: &str) -> Result<(), String> {
    let result = sqlx::query(sqlx::AssertSqlSafe(statement.to_string()))
        .execute(admin)
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(e) if is_duplicate_database(&e) => Ok(()),
        Err(e) => Err(format!("{statement} failed: {e}")),
    }
}

/// Whether `e` is PostgreSQL's `duplicate_database` (SQLSTATE 42P04).
fn is_duplicate_database(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| code == "42P04")
}

/// Whether `name` is safe to interpolate into a `CREATE DATABASE`
/// statement as a bare identifier - letters, digits and underscores,
/// non-empty. Every current caller passes a `&str` literal, so this only
/// ever fires if one is later built from something else.
fn is_plain_identifier(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `database_name` on this process's one embedded server - started on
/// first use, the database created if it doesn't exist yet - ignoring
/// `DATABASE_URL`. The embedded-server path [`database_url`] falls back
/// to, and the reason `DATABASE_URL`'s database name being ignored
/// matters: with no `DATABASE_URL` there is one server per process, so
/// the database name is the only thing keeping two binaries apart.
/// There is nothing to drop here - the server itself is per-process, so
/// its databases are as empty as this one needs to be.
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

/// The directory holding the embedded server's own client programs
/// (`pgbench`, `psql`), or [`None`] if this process hasn't started one -
/// with `DATABASE_URL` set, it never does. For benchmarks that drive the
/// server with those programs rather than through sqlx.
pub async fn embedded_binary_dir() -> Option<std::path::PathBuf> {
    let server = EMBEDDED.get()?.as_ref()?.lock().await;
    Some(server.settings().binary_dir())
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

/// A Postgres server of a test's own that it can fail over: take a base
/// backup, keep committing on the primary, then stop the primary and
/// promote the backup in its place, on the same port - what a client
/// behind a virtual IP or DNS name sees when an asynchronous standby is
/// promoted. Everything committed after the backup is gone, and the
/// promoted server is on a new timeline (docs/architecture.md §176).
///
/// Never `DATABASE_URL`'s server: failing that over isn't a test's to do.
/// The standby has no `primary_conninfo`, so it replays only what the
/// backup holds. Both servers and their data dirs are cleaned up when this
/// drops, and by a watchdog if the process dies first, like
/// [`database_url`]'s embedded server.
///
/// Postgres refuses to run as root, and CI runs tests as root. There this
/// uses the system's server binaries (`/usr/lib/postgresql/*/bin`, or
/// `SKILJ_TEST_PGBIN`) and runs every server command as the `postgres`
/// user, the way scripts/ci-postgres-local.sh does; the embedded binaries
/// live in root's home, out of that user's reach. Anywhere else it uses
/// the embedded server.
pub struct FailoverServer {
    bin_dir: std::path::PathBuf,
    /// `Some("postgres")` when running as root.
    run_as: Option<String>,
    primary_dir: std::path::PathBuf,
    standby_dir: std::path::PathBuf,
    /// What [`Drop`] removes: the system path's own base dir. The embedded
    /// server removes its data dir itself.
    owned_dir: Option<std::path::PathBuf>,
    port: u16,
    /// `-o` options for `pg_ctl start`, the same for both servers.
    server_options: String,
    /// Kept alive so its data dir isn't removed under the primary.
    _embedded: Option<PostgreSQL>,
}

impl FailoverServer {
    /// Starts the primary with a database `database_name` on it, and
    /// returns that database's URL. `None` - with the reason printed after
    /// `skipping:` - if no server can be set up.
    pub async fn start(database_name: &str) -> Option<(Self, String)> {
        let started = if running_as_root() {
            Self::start_system(database_name).await
        } else {
            Self::start_embedded(database_name).await
        };
        match started {
            Ok(started) => Some(started),
            Err(e) => {
                eprintln!("skipping: a PostgreSQL server for a failover test failed: {e}");
                None
            }
        }
    }

    async fn start_embedded(database_name: &str) -> Result<(Self, String), String> {
        let mut primary = PostgreSQL::default();
        primary
            .setup()
            .await
            .map_err(|e| format!("embedded setup: {e}"))?;
        spawn_cleanup_watchdog(&primary);
        primary
            .start()
            .await
            .map_err(|e| format!("embedded start: {e}"))?;
        primary
            .create_database(database_name)
            .await
            .map_err(|e| format!("embedded create_database: {e}"))?;
        let settings = primary.settings();
        let primary_dir = settings.data_dir.clone();
        let standby_dir = primary_dir.with_file_name(format!(
            "{}-standby",
            primary_dir
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
        ));
        let mut server = Self {
            bin_dir: settings.binary_dir(),
            run_as: None,
            primary_dir,
            standby_dir,
            owned_dir: None,
            port: settings.port,
            server_options: format!("-F -p {}", settings.port),
            _embedded: None,
        };
        server.spawn_watchdog();
        let url = settings.url(database_name);
        server._embedded = Some(primary);
        Ok((server, url))
    }

    async fn start_system(database_name: &str) -> Result<(Self, String), String> {
        let bin_dir = system_pg_bin().ok_or(
            "running as root, and no system PostgreSQL server binaries were found \
             (install `postgresql`, or set SKILJ_TEST_PGBIN)",
        )?;
        let user = "postgres".to_string();
        let base = std::env::temp_dir().join(format!(
            "skilj-failover-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&base).map_err(|e| format!("creating {base:?}: {e}"))?;
        run(std::process::Command::new("chown")
            .arg(format!("{user}:{user}"))
            .arg(&base))?;
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .map_err(|e| format!("finding a free port: {e}"))?
            .port();
        let server = Self {
            bin_dir,
            run_as: Some(user),
            primary_dir: base.join("primary"),
            standby_dir: base.join("standby"),
            owned_dir: Some(base.clone()),
            port,
            server_options: format!(
                "-F -p {port} -k {} -c listen_addresses=127.0.0.1",
                base.display()
            ),
            _embedded: None,
        };
        server.spawn_watchdog();
        run(server
            .command("initdb")
            .args(["-A", "trust", "-U", "postgres", "-D"])
            .arg(&server.primary_dir))?;
        server.pg_ctl_start(&server.primary_dir)?;
        let admin = format!("postgres://postgres@127.0.0.1:{port}/postgres");
        let pool = sqlx::PgPool::connect(&admin)
            .await
            .map_err(|e| format!("connecting to the primary: {e}"))?;
        run_as_admin(&pool, &format!("CREATE DATABASE {database_name}")).await?;
        pool.close().await;
        Ok((
            server,
            format!("postgres://postgres@127.0.0.1:{port}/{database_name}"),
        ))
    }

    /// Takes the base backup the standby will be promoted from: it has
    /// everything committed up to now, and nothing committed after.
    pub fn base_backup(&self) -> Result<(), String> {
        let mut command = self.command("pg_basebackup");
        command
            .args(["-h", "127.0.0.1", "-p"])
            .arg(self.port.to_string())
            .arg("-D")
            .arg(&self.standby_dir)
            .args(["-X", "stream"])
            // Not the default spread checkpoint, which waits most of
            // `checkpoint_timeout` before the backup starts.
            .arg("--checkpoint=fast");
        if let Some(embedded) = &self._embedded {
            let settings = embedded.settings();
            command
                .arg("-U")
                .arg(&settings.username)
                .env("PGPASSWORD", &settings.password);
        } else {
            command.args(["-U", "postgres"]);
        }
        run(&mut command)?;
        run(self
            .command("touch")
            .arg(self.standby_dir.join("standby.signal")))
    }

    /// Stops the primary at once and promotes the base backup in its place,
    /// on the same port. Connections to the old primary fail, and new ones
    /// reach the promoted server.
    pub fn fail_over(&self) -> Result<(), String> {
        run(self
            .command("pg_ctl")
            .args(["stop", "-m", "immediate", "-w", "-D"])
            .arg(&self.primary_dir))?;
        self.pg_ctl_start(&self.standby_dir)?;
        run(self
            .command("pg_ctl")
            .args(["promote", "-w", "-D"])
            .arg(&self.standby_dir))
    }

    fn pg_ctl_start(&self, data_dir: &std::path::Path) -> Result<(), String> {
        run(self
            .command("pg_ctl")
            .args(["start", "-w", "-D"])
            .arg(data_dir)
            .arg("-l")
            .arg(data_dir.with_extension("log"))
            .arg("-o")
            .arg(&self.server_options))
    }

    /// `program` - a server binary, or `touch` - run as the server's user.
    fn command(&self, program: &str) -> std::process::Command {
        let program = if program == "touch" {
            std::path::PathBuf::from("touch")
        } else {
            self.bin_dir.join(program)
        };
        match &self.run_as {
            Some(user) => {
                let mut command = std::process::Command::new("runuser");
                command.args(["-u", user, "--"]).arg(program);
                command
            }
            None => std::process::Command::new(program),
        }
    }

    /// Stops whatever runs from the standby dir (and, on the system path,
    /// the primary) and removes the dirs once this process is gone.
    fn spawn_watchdog(&self) {
        let pg_ctl = self.bin_dir.join("pg_ctl");
        let run_as = match &self.run_as {
            Some(user) => format!("runuser -u {user} --"),
            None => String::new(),
        };
        let owned = self
            .owned_dir
            .clone()
            .unwrap_or_else(|| self.standby_dir.clone());
        spawn_watchdog(
            "failover test PostgreSQL",
            &format!(
                r#"{run_as} "$2" stop -D "$3" -m immediate -w
                   {run_as} "$2" stop -D "$4" -m immediate -w
                   rm -rf "$5""#
            ),
            &[
                pg_ctl.as_os_str(),
                self.standby_dir.as_os_str(),
                self.primary_dir.as_os_str(),
                owned.as_os_str(),
            ],
        );
    }
}

impl Drop for FailoverServer {
    fn drop(&mut self) {
        for data_dir in [&self.standby_dir, &self.primary_dir] {
            if self._embedded.is_some() && data_dir == &self.primary_dir {
                continue; // the embedded server stops and removes its own
            }
            let _ = self
                .command("pg_ctl")
                .args(["stop", "-m", "immediate", "-w", "-D"])
                .arg(data_dir)
                .output();
        }
        let _ = std::fs::remove_dir_all(&self.standby_dir);
        if let Some(owned) = &self.owned_dir {
            let _ = std::fs::remove_dir_all(owned);
        }
    }
}

/// Whether this process runs as root, which Postgres refuses to.
fn running_as_root() -> bool {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .is_ok_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "0")
}

/// The system's Postgres server binaries: `SKILJ_TEST_PGBIN`, else the
/// newest `/usr/lib/postgresql/<version>/bin` (Debian's layout, what CI
/// installs), else wherever `pg_ctl` is on the `PATH`.
fn system_pg_bin() -> Option<std::path::PathBuf> {
    if let Ok(dir) = std::env::var("SKILJ_TEST_PGBIN") {
        return Some(dir.into());
    }
    let newest = std::fs::read_dir("/usr/lib/postgresql")
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let version: u32 = entry.file_name().to_string_lossy().parse().ok()?;
            let bin = entry.path().join("bin");
            bin.join("pg_ctl").exists().then_some((version, bin))
        })
        .max_by_key(|(version, _)| *version)
        .map(|(_, bin)| bin);
    newest.or_else(|| {
        let output = std::process::Command::new("sh")
            .args(["-c", "command -v pg_ctl"])
            .output()
            .ok()?;
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        std::path::Path::new(&path)
            .parent()
            .map(|dir| dir.to_path_buf())
    })
}

/// Runs `command` to completion; its output in the error if it fails.
fn run(command: &mut std::process::Command) -> Result<(), String> {
    let output = command
        .output()
        .map_err(|e| format!("running {command:?} failed: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{command:?} failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}
