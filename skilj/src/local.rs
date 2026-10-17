//! Codeberg issue #100 - a supported single-user local mode.
//!
//! `postgresql_embedded` (already used by the test harnesses) starts a
//! real Postgres 18 server in a data directory of this process's own,
//! creates one database, and hands back its URL. Nothing here is a
//! different storage engine or an in-memory shortcut: it is the same
//! Postgres schema, migrations and behaviour as a production deployment,
//! with the server owned by this process.
//!
//! The data directory persists between runs by default (`cleanup: false`);
//! `cleanup: true` deletes it when
//! [`LocalPostgres::shutdown`](crate::local::LocalPostgres::shutdown) runs
//! (or on `Drop`, via `postgresql_embedded`'s own `temporary` setting). The
//! server listens on `127.0.0.1` only, with a fixed `postgres`/
//! `skilj-local` credential - a local-only convenience, not a secret, and
//! deliberately not exposed on a network interface.
//!
//! Enabled by the `local` feature, so a consuming application that
//! already brings its own Postgres pays nothing for it.

use postgresql_embedded::{PostgreSQL, Settings};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// This mode's fixed local-only superuser credential. `postgresql_embedded`
/// always initializes the `postgres` role (`BOOTSTRAP_SUPERUSER`); the
/// password is fixed so a data directory survives a restart, unlike the
/// crate's random default.
const LOCAL_USER: &str = "postgres";
const LOCAL_PASSWORD: &str = "skilj-local";
const DEFAULT_DATABASE: &str = "skilj";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalConfig {
    /// Where Postgres keeps its data. Created if missing.
    pub data_dir: PathBuf,
    /// `None`/`Some(0)` asks Postgres for an ephemeral port.
    pub port: Option<u16>,
    pub database: String,
    /// Delete `data_dir` on shutdown.
    pub cleanup: bool,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            port: None,
            database: DEFAULT_DATABASE.to_string(),
            cleanup: false,
        }
    }
}

impl LocalConfig {
    /// `SKILJ_LOCAL_DATA_DIR`, `SKILJ_LOCAL_PORT`, `SKILJ_LOCAL_DATABASE`
    /// and `SKILJ_LOCAL_CLEANUP` (`true`/`1`), with the defaults above for
    /// anything unset. A malformed value is an error rather than silently
    /// ignored, so a typo never starts a server somewhere unexpected.
    pub fn from_env() -> Result<Self, Error> {
        let mut config = Self::default();
        if let Some(value) = std::env::var_os("SKILJ_LOCAL_DATA_DIR") {
            config.data_dir = PathBuf::from(value);
        }
        if let Some(value) = std::env::var_os("SKILJ_LOCAL_PORT") {
            let value = value.to_string_lossy();
            config.port = Some(value.parse().map_err(|_| Error::InvalidConfig {
                variable: "SKILJ_LOCAL_PORT".to_string(),
                value: value.to_string(),
            })?);
        }
        if let Some(value) = std::env::var_os("SKILJ_LOCAL_DATABASE") {
            config.database = value.to_string_lossy().to_string();
        }
        if let Some(value) = std::env::var_os("SKILJ_LOCAL_CLEANUP") {
            let value = value.to_string_lossy();
            config.cleanup = match value.to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => true,
                "0" | "false" | "no" | "off" => false,
                _ => {
                    return Err(Error::InvalidConfig {
                        variable: "SKILJ_LOCAL_CLEANUP".to_string(),
                        value: value.to_string(),
                    })
                }
            };
        }
        config.validate()?;
        Ok(config)
    }

    /// Rejects a database name that cannot be safely placed in the URL
    /// and `CREATE DATABASE` statement. Called by [from_env](Self::from_env)
    /// and [start](LocalPostgres::start), so a hand-built config is
    /// checked too.
    pub fn validate(&self) -> Result<(), Error> {
        if self.database.is_empty()
            || !self
                .database
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(Error::InvalidDatabase {
                database: self.database.clone(),
            });
        }
        Ok(())
    }
}

pub fn default_data_dir() -> PathBuf {
    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
        return PathBuf::from(local_app_data)
            .join("skilj")
            .join("postgresql");
    }
    if let Some(xdg) = std::env::var_os("XDG_DATA_HOME") {
        return PathBuf::from(xdg).join("skilj").join("postgresql");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("skilj")
            .join("postgresql");
    }
    PathBuf::from(".skilj").join("postgresql")
}

/// A running embedded Postgres. Keep it alive for as long as the `Skilj`
/// built against its URL is in use; call [shutdown](Self::shutdown) (or
/// drop it) to stop the server.
enum Backend {
    Embedded(Box<PostgreSQL>),
    /// System server binaries run as the `postgres` user, for when this
    /// process is root - which `initdb`/`postgres` refuse to run as.
    System {
        bin_dir: PathBuf,
    },
}

pub struct LocalPostgres {
    server: Backend,
    database_url: String,
    data_dir: PathBuf,
    cleanup: bool,
}

impl LocalPostgres {
    /// Installs (first run only), initializes, starts, and creates the
    /// configured database if it does not exist yet.
    pub async fn start(config: LocalConfig) -> Result<Self, Error> {
        config.validate()?;
        if running_as_root() {
            return Self::start_system(config).await;
        }
        Self::start_embedded(config).await
    }

    async fn start_embedded(config: LocalConfig) -> Result<Self, Error> {
        std::fs::create_dir_all(&config.data_dir).map_err(|source| Error::Io {
            path: config.data_dir.clone(),
            source,
        })?;

        let mut settings = Settings::new();
        settings.data_dir = config.data_dir.clone();
        // Outside `data_dir`: `initdb` refuses a directory that already
        // has anything in it, and `initialize` writes this file before it
        // runs.
        settings.password_file = config.data_dir.with_extension("pgpass");
        settings.host = "127.0.0.1".to_string();
        settings.port = config.port.unwrap_or(0);
        settings.username = LOCAL_USER.to_string();
        settings.password = LOCAL_PASSWORD.to_string();
        // `temporary` is `postgresql_embedded`'s own cleanup switch; a
        // persistent local mode leaves it false, a throwaway one true.
        settings.temporary = config.cleanup;
        settings.timeout = Some(Duration::from_secs(60));

        let mut server = PostgreSQL::new(settings);
        server.setup().await?;
        server.start().await?;
        if !server.database_exists(&config.database).await? {
            server.create_database(&config.database).await?;
        }
        let database_url = server.settings().url(&config.database);
        Ok(Self {
            server: Backend::Embedded(Box::new(server)),
            database_url,
            data_dir: config.data_dir,
            cleanup: config.cleanup,
        })
    }

    /// Root fallback: `initdb` and `postgres` refuse to run as root, so
    /// the system's server binaries are run as the `postgres` user via
    /// `runuser` - the same arrangement as `scripts/ci-postgres-local.sh`
    /// and `skilj_test_support::FailoverServer`.
    async fn start_system(config: LocalConfig) -> Result<Self, Error> {
        let bin_dir = system_pg_bin().ok_or_else(|| Error::InvalidConfig {
            variable: "SKILJ_TEST_PGBIN".to_string(),
            value: "running as root, and no system PostgreSQL server binaries were found (install `postgresql`, or set SKILJ_TEST_PGBIN)".to_string(),
        })?;
        std::fs::create_dir_all(&config.data_dir).map_err(|source| Error::Io {
            path: config.data_dir.clone(),
            source,
        })?;
        run_as_postgres_output(
            tokio::process::Command::new("chown")
                .arg("postgres:postgres")
                .arg(&config.data_dir),
        )
        .await?;
        // Trust auth on the loopback socket: a fixed local-only
        // convenience, matching the embedded path's fixed credential.
        let socket_dir = config.data_dir.join("sockets");
        std::fs::create_dir_all(&socket_dir).map_err(|source| Error::Io {
            path: socket_dir.clone(),
            source,
        })?;
        run_as_postgres_output(
            tokio::process::Command::new("chown")
                .arg("postgres:postgres")
                .arg(&socket_dir),
        )
        .await?;
        if !config.data_dir.join("PG_VERSION").exists() {
            let mut initdb = as_postgres(&bin_dir, "initdb");
            initdb
                .args(["-A", "trust", "-U", "postgres", "-D"])
                .arg(&config.data_dir);
            run_as_postgres_output(&mut initdb).await?;
        }
        let port = match config.port {
            Some(port) if port != 0 => port,
            _ => free_port()?,
        };
        let options = format!(
            "-F -p {port} -k {} -c listen_addresses=127.0.0.1",
            socket_dir.display()
        );
        let log = config.data_dir.with_extension("log");
        let mut start = as_postgres(&bin_dir, "pg_ctl");
        start
            .args(["start", "-w", "-t", "60", "-D"])
            .arg(&config.data_dir)
            .arg("-l")
            .arg(&log)
            .arg("-o")
            .arg(&options);
        run_as_postgres_output(&mut start).await?;
        // `config.validate` already restricts the name to ASCII letters,
        // digits, `_` and `-`, so interpolating it is safe.
        let check = format!(
            "SELECT 1 FROM pg_database WHERE datname='{}'",
            config.database
        );
        let mut query = as_postgres(&bin_dir, "psql");
        query.args([
            "-h",
            "127.0.0.1",
            "-p",
            &port.to_string(),
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-tAc",
            &check,
        ]);
        let exists = run_as_postgres_output(&mut query).await?;
        if String::from_utf8_lossy(&exists.stdout).trim() != "1" {
            let mut createdb = as_postgres(&bin_dir, "createdb");
            createdb
                .args(["-h", "127.0.0.1", "-p", &port.to_string(), "-U", "postgres"])
                .arg(&config.database);
            run_as_postgres_output(&mut createdb).await?;
        }
        let database_url = format!("postgres://postgres@127.0.0.1:{port}/{}", config.database);
        Ok(Self {
            server: Backend::System { bin_dir },
            database_url,
            data_dir: config.data_dir,
            cleanup: config.cleanup,
        })
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Stops the server, and removes the data directory when
    /// `cleanup` was requested.
    pub async fn shutdown(self) -> Result<(), Error> {
        match &self.server {
            Backend::Embedded(server) => {
                server.stop().await?;
            }
            Backend::System { bin_dir, .. } => {
                let mut stop = as_postgres(bin_dir, "pg_ctl");
                stop.args(["stop", "-m", "immediate", "-w", "-D"])
                    .arg(&self.data_dir);
                run_as_postgres_output(&mut stop).await?;
            }
        }
        drop(self.server);
        if self.cleanup {
            match std::fs::remove_dir_all(&self.data_dir) {
                Ok(()) => {}
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(Error::Io {
                        path: self.data_dir.clone(),
                        source,
                    })
                }
            }
        }
        Ok(())
    }
}

/// `program` from `bin_dir` run as the `postgres` user.
fn as_postgres(bin_dir: &Path, program: &str) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("runuser");
    command
        .args(["-u", "postgres", "--"])
        .arg(bin_dir.join(program));
    command
}

async fn run_as_postgres_output(
    command: &mut tokio::process::Command,
) -> Result<std::process::Output, Error> {
    let output = command.output().await.map_err(|source| Error::Io {
        path: PathBuf::from("runuser -u postgres"),
        source,
    })?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(Error::Command {
            command: "runuser -u postgres".to_string(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }
}

fn free_port() -> Result<u16, Error> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .map_err(Error::IoSocket)
}

/// Whether this process runs as root, which Postgres refuses to.
fn running_as_root() -> bool {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .is_ok_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "0")
}

/// The system's Postgres server binaries: `SKILJ_TEST_PGBIN`, else the
/// newest `/usr/lib/postgresql/<version>/bin` (Debian's layout), else
/// wherever `pg_ctl` is on the `PATH`.
fn system_pg_bin() -> Option<PathBuf> {
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("local Postgres: {0}")]
    Embedded(#[from] postgresql_embedded::Error),

    #[error("local Postgres: {command} failed: stdout={stdout} stderr={stderr}")]
    Command {
        command: String,
        stdout: String,
        stderr: String,
    },

    #[error("local Postgres: cannot bind a socket: {0}")]
    IoSocket(#[source] std::io::Error),

    #[error("local Postgres: {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("local Postgres: {variable}={value:?} is not valid")]
    InvalidConfig { variable: String, value: String },

    #[error(
        "local Postgres: database name {database:?} must be ASCII letters, digits, '_' or '-'"
    )]
    InvalidDatabase { database: String },
}
