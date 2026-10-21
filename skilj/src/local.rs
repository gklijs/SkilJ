//! issue #100 - a supported single-user local mode.
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
//! server listens on `127.0.0.1` only (`listen_addresses`), never on a
//! network interface.
//!
//! **The password is a real secret.** The login is the cluster's
//! superuser, and anyone who can log in as a Postgres superuser can run
//! programs as the user the server runs as (`COPY ... TO PROGRAM`) and read
//! every private field in plaintext. Listening on `127.0.0.1` keeps other
//! machines out, not other processes or users on this one. So the password
//! is generated randomly (256 bits) on first start and kept in
//! `<data_dir>.pgpass`, readable by the owner only (`0600` on Unix), and
//! read back on every later start so the same data directory keeps working.
//!
//! **The Postgres major version is pinned** ([`postgres_version`](crate::local::postgres_version)). A data
//! directory can only be opened by the major version that created it; left
//! unpinned, `postgresql_embedded` installs the newest release it finds, so
//! a new machine or a cleared install cache could fetch a newer major and
//! fail to start on existing data. Moving to a newer major is a deliberate
//! dump-and-restore, not something a dependency update should do. For
//! now it is pinned to one exact release, 18.6.0, working around an
//! upstream bug in 18.6.1 (docs/architecture.md §224).
//!
//! Enabled by the `local` feature, so a consuming application that
//! already brings its own Postgres pays nothing for it.

use postgresql_embedded::{PostgreSQL, Settings, VersionReq};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// `postgresql_embedded` always initializes the `postgres` role
/// (`BOOTSTRAP_SUPERUSER`).
const LOCAL_USER: &str = "postgres";
const DEFAULT_DATABASE: &str = "skilj";

/// The Postgres version local mode installs and runs: exactly 18.6.0 for
/// now, and never 19. See this module's own docs.
///
/// Exactly 18.6.0 rather than any 18.x because of an upstream bug
/// (theseus-rs/postgresql-embedded#263): the 18.6.1 builds bundle their
/// own timezone data, with `UTC` and `Etc/UTC` as tar hardlinks that
/// `postgresql_archive`'s extractor skips, so every connection asking for
/// `TimeZone=UTC` - sqlx's, and `postgresql_embedded`'s own - is refused.
/// 18.6.0 uses the system's `/usr/share/zoneinfo` instead. Relax this back
/// to `postgresql_embedded::V18` once a fixed release is out.
pub fn postgres_version() -> VersionReq {
    VersionReq::parse("=18.6.0").expect("a valid version requirement")
}

/// Where and how [`LocalPostgres::start`] runs its server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalConfig {
    /// Where Postgres keeps its data. Created if missing.
    pub data_dir: PathBuf,
    /// `None`/`Some(0)` asks Postgres for an ephemeral port.
    pub port: Option<u16>,
    /// The database skilj uses, created if missing.
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

/// The platform's per-user data directory, `skilj/postgresql` under it:
/// `%LOCALAPPDATA%`, else `$XDG_DATA_HOME`, else `~/.local/share`.
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

enum Backend {
    Embedded(Box<PostgreSQL>),
    /// System server binaries run as the `postgres` user, for when this
    /// process is root - which `initdb`/`postgres` refuse to run as.
    System {
        bin_dir: PathBuf,
    },
}

/// A running local Postgres. Keep it alive for as long as the `Skilj`
/// built against its URL is in use; call [shutdown](Self::shutdown) (or
/// drop it) to stop the server.
pub struct LocalPostgres {
    server: Backend,
    database_url: String,
    display_url: String,
    data_dir: PathBuf,
    cleanup: bool,
}

/// `<data_dir>.pgpass` - outside `data_dir`, because `initdb` refuses a
/// directory that already has anything in it.
pub fn password_file(data_dir: &Path) -> PathBuf {
    data_dir.with_extension("pgpass")
}

/// Reads the password from `path`, or, the first time, generates one and
/// writes it there owner-only. `postgresql_embedded` only writes its own
/// password file when none exists, so creating it here first is what
/// decides its permissions. An existing file readable by anyone else is
/// narrowed to owner-only rather than trusted as is.
pub fn load_or_create_password(path: &Path) -> Result<String, Error> {
    let io_error = |source| Error::Io {
        path: path.to_path_buf(),
        source,
    };
    if path.exists() {
        restrict_to_owner(path).map_err(io_error)?;
        let password = std::fs::read_to_string(path).map_err(io_error)?;
        let password = password.trim().to_string();
        if password.is_empty() {
            return Err(Error::EmptyPasswordFile {
                path: path.to_path_buf(),
            });
        }
        return Ok(password);
    }
    let password = skilj_core::shared::generate_token_secret();
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(io_error)?;
    file.write_all(password.as_bytes()).map_err(io_error)?;
    Ok(password)
}

#[cfg(unix)]
fn restrict_to_owner(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)?.permissions().mode();
    if mode & 0o077 != 0 {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// On Windows the file lives under the user's own profile directory, whose
/// default ACL already keeps other users out.
#[cfg(not(unix))]
fn restrict_to_owner(_path: &Path) -> std::io::Result<()> {
    Ok(())
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

        let password_file = password_file(&config.data_dir);
        let password = load_or_create_password(&password_file)?;

        let mut settings = Settings::new();
        settings.version = postgres_version();
        settings.data_dir = config.data_dir.clone();
        settings.password_file = password_file;
        settings.host = "127.0.0.1".to_string();
        // `host` only shapes the URL; this is what the server binds to.
        settings
            .configuration
            .insert("listen_addresses".to_string(), "127.0.0.1".to_string());
        settings.port = config.port.unwrap_or(0);
        settings.username = LOCAL_USER.to_string();
        settings.password = password.clone();
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
        let display_url = database_url.replace(&password, "***");
        Ok(Self {
            server: Backend::Embedded(Box::new(server)),
            database_url,
            display_url,
            data_dir: config.data_dir,
            cleanup: config.cleanup,
        })
    }

    /// Root fallback: `initdb` and `postgres` refuse to run as root, so
    /// the system's server binaries are run as the `postgres` user via
    /// `runuser` - the same arrangement as `scripts/ci-postgres-local.sh`
    /// and `skilj_test_support::FailoverServer`.
    ///
    /// Same login rules as the embedded path: TCP on `127.0.0.1` needs the
    /// random password from `<data_dir>.pgpass` (`scram-sha-256`), set by
    /// `initdb` itself. This process's own setup commands use a Unix socket
    /// in `<data_dir>.sockets`, owner-only, with `peer` authentication - so
    /// only the operating-system user `postgres` gets in that way. Never
    /// `trust`: on a shared machine that would let any local process in as
    /// superuser with no password at all.
    async fn start_system(config: LocalConfig) -> Result<Self, Error> {
        let existing_major = std::fs::read_to_string(config.data_dir.join("PG_VERSION"))
            .ok()
            .and_then(|version| version.trim().parse::<u32>().ok());
        let bin_dir = system_pg_bin(existing_major).ok_or_else(|| Error::InvalidConfig {
            variable: "SKILJ_TEST_PGBIN".to_string(),
            value: "running as root, and no system PostgreSQL server binaries were found (install `postgresql`, or set SKILJ_TEST_PGBIN)".to_string(),
        })?;
        // Outside `data_dir`: `initdb` refuses a directory that already has
        // anything in it.
        let socket_dir = config.data_dir.with_extension("sockets");
        // Root hands these to `postgres`; a symlink planted at either path
        // (a data directory in a shared, writable place) would hand over
        // whatever it points at instead.
        refuse_symlink(&config.data_dir)?;
        refuse_symlink(&socket_dir)?;
        std::fs::create_dir_all(&config.data_dir).map_err(|source| Error::Io {
            path: config.data_dir.clone(),
            source,
        })?;
        chown_to_postgres(&config.data_dir).await?;
        std::fs::create_dir_all(&socket_dir).map_err(|source| Error::Io {
            path: socket_dir.clone(),
            source,
        })?;
        set_owner_only_dir(&socket_dir)?;
        chown_to_postgres(&socket_dir).await?;

        let password = load_or_create_password(&password_file(&config.data_dir))?;
        if existing_major.is_none() {
            // `initdb` reads the superuser password from a file it can open
            // as `postgres`; the `.pgpass` file stays root-only, so this is a
            // short-lived owner-only copy, removed straight after.
            let pwfile = config.data_dir.with_extension("initpw");
            let _ = std::fs::remove_file(&pwfile);
            {
                let mut options = std::fs::OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let io_error = |source| Error::Io {
                    path: pwfile.clone(),
                    source,
                };
                let mut file = options.open(&pwfile).map_err(io_error)?;
                file.write_all(password.as_bytes()).map_err(io_error)?;
            }
            let initialized = async {
                chown_to_postgres(&pwfile).await?;
                let mut initdb = as_postgres(&bin_dir, "initdb");
                initdb
                    .args([
                        "--auth-host=scram-sha-256",
                        "--auth-local=peer",
                        "-U",
                        LOCAL_USER,
                    ])
                    .arg(format!("--pwfile={}", pwfile.display()))
                    .arg("-D")
                    .arg(&config.data_dir);
                run_as_postgres_output(&mut initdb).await
            }
            .await;
            let _ = std::fs::remove_file(&pwfile);
            initialized?;
        } else {
            refuse_trust_authentication(&config.data_dir)?;
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
        let socket = socket_dir.display().to_string();
        let port_arg = port.to_string();
        // `config.validate` already restricts the name to ASCII letters,
        // digits, `_` and `-`, so interpolating it is safe.
        let check = format!(
            "SELECT 1 FROM pg_database WHERE datname='{}'",
            config.database
        );
        let mut query = as_postgres(&bin_dir, "psql");
        query.args([
            "-h", &socket, "-p", &port_arg, "-U", LOCAL_USER, "-d", "postgres", "-tAc", &check,
        ]);
        let exists = run_as_postgres_output(&mut query).await?;
        if String::from_utf8_lossy(&exists.stdout).trim() != "1" {
            let mut createdb = as_postgres(&bin_dir, "createdb");
            createdb
                .args(["-h", &socket, "-p", &port_arg, "-U", LOCAL_USER])
                .arg(&config.database);
            run_as_postgres_output(&mut createdb).await?;
        }
        let database_url = format!(
            "postgres://{LOCAL_USER}:{password}@127.0.0.1:{port}/{}",
            config.database
        );
        let display_url = database_url.replace(&password, "***");
        Ok(Self {
            server: Backend::System { bin_dir },
            database_url,
            display_url,
            data_dir: config.data_dir,
            cleanup: config.cleanup,
        })
    }

    /// The connection URL, **including the superuser password** - pass it
    /// to `Skilj`, never print or log it; use [display_url](Self::display_url)
    /// for that.
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// [database_url](Self::database_url) with the password replaced by
    /// `***`, safe to print.
    pub fn display_url(&self) -> &str {
        &self.display_url
    }

    /// Where this server keeps its data.
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
                // `fast`, not `immediate`: a clean shutdown, so the next
                // start needs no crash recovery.
                stop.args(["stop", "-m", "fast", "-w", "-D"])
                    .arg(&self.data_dir);
                run_as_postgres_output(&mut stop).await?;
            }
        }
        drop(self.server);
        if self.cleanup {
            // Everything this mode keeps next to the data directory too -
            // above all the password file, which must not outlive the data.
            for path in [
                self.data_dir.clone(),
                password_file(&self.data_dir),
                self.data_dir.with_extension("sockets"),
                self.data_dir.with_extension("log"),
            ] {
                let removed = if path.is_dir() {
                    std::fs::remove_dir_all(&path)
                } else {
                    std::fs::remove_file(&path)
                };
                match removed {
                    Ok(()) => {}
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => return Err(Error::Io { path, source }),
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

async fn chown_to_postgres(path: &Path) -> Result<(), Error> {
    run_as_postgres_output(
        // `-h`: change a symlink itself, never what it points at.
        tokio::process::Command::new("chown")
            .arg("-h")
            .arg("postgres:postgres")
            .arg(path),
    )
    .await
    .map(|_| ())
}

fn refuse_symlink(path: &Path) -> Result<(), Error> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(Error::SymbolicLink {
            path: path.to_path_buf(),
        }),
        _ => Ok(()),
    }
}

#[cfg(unix)]
fn set_owner_only_dir(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(|source| {
        Error::Io {
            path: path.to_path_buf(),
            source,
        }
    })
}

#[cfg(not(unix))]
fn set_owner_only_dir(_path: &Path) -> Result<(), Error> {
    Ok(())
}

/// A data directory an earlier development build initialized with `trust`
/// authentication lets any local process in as superuser. Refused rather
/// than silently kept; it has no password to migrate to.
fn refuse_trust_authentication(data_dir: &Path) -> Result<(), Error> {
    let hba = data_dir.join("pg_hba.conf");
    let Ok(contents) = std::fs::read_to_string(&hba) else {
        return Ok(());
    };
    let trusts = contents.lines().any(|line| {
        let line = line.split('#').next().unwrap_or("");
        line.split_whitespace().any(|word| word == "trust")
    });
    if trusts {
        return Err(Error::TrustAuthentication { path: hba });
    }
    Ok(())
}

/// Whether this process runs as root, which Postgres refuses to.
fn running_as_root() -> bool {
    std::process::Command::new("id")
        .arg("-u")
        .output()
        .is_ok_and(|output| String::from_utf8_lossy(&output.stdout).trim() == "0")
}

/// The system's Postgres server binaries: `SKILJ_TEST_PGBIN`; else, for
/// an existing data directory, the `/usr/lib/postgresql/<major>/bin` that
/// matches its `PG_VERSION` (a data directory only opens under the major
/// version that created it); else the newest one there (Debian's layout);
/// else wherever `pg_ctl` is on the `PATH`.
fn system_pg_bin(existing_major: Option<u32>) -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("SKILJ_TEST_PGBIN") {
        return Some(dir.into());
    }
    if let Some(major) = existing_major {
        let bin = PathBuf::from(format!("/usr/lib/postgresql/{major}/bin"));
        if bin.join("pg_ctl").exists() {
            return Some(bin);
        }
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

/// Why local mode could not start or stop its server.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The embedded server failed to install, start or stop.
    #[error("local Postgres: {0}")]
    Embedded(#[from] postgresql_embedded::Error),

    /// A system Postgres command (root mode) exited unsuccessfully.
    #[error("local Postgres: {command} failed: stdout={stdout} stderr={stderr}")]
    Command {
        /// The program that failed.
        command: String,
        /// Its standard output.
        stdout: String,
        /// Its standard error.
        stderr: String,
    },

    /// No free local port could be found.
    #[error("local Postgres: cannot bind a socket: {0}")]
    IoSocket(#[source] std::io::Error),

    /// A file or directory local mode manages could not be used.
    #[error("local Postgres: {path}: {source}")]
    Io {
        /// The file or directory.
        path: PathBuf,
        /// What went wrong.
        #[source]
        source: std::io::Error,
    },

    /// An environment variable (or the system binaries it names) is not
    /// usable.
    #[error("local Postgres: {variable}={value:?} is not valid")]
    InvalidConfig {
        /// The variable's name.
        variable: String,
        /// Its value, or why nothing usable was found.
        value: String,
    },

    /// The configured database name has characters other than ASCII
    /// letters, digits, `_` and `-`.
    #[error(
        "local Postgres: database name {database:?} must be ASCII letters, digits, '_' or '-'"
    )]
    InvalidDatabase {
        /// The refused name.
        database: String,
    },

    /// The password file exists but holds no password.
    #[error("local Postgres: password file {path} is empty")]
    EmptyPasswordFile {
        /// The password file.
        path: PathBuf,
    },

    /// Running as root, the data or socket directory is a symbolic link,
    /// which root would hand to the `postgres` user.
    #[error(
        "local Postgres: {path} is a symbolic link; running as root, local mode only uses a \
         real directory"
    )]
    SymbolicLink {
        /// The linked path.
        path: PathBuf,
    },

    /// An existing data directory allows `trust` logins.
    #[error(
        "local Postgres: {path} allows `trust` logins, which let any local process in as \
         superuser without a password; this data directory was initialized by an earlier \
         development build - move it aside and start again"
    )]
    TrustAuthentication {
        /// The data directory's `pg_hba.conf`.
        path: PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "skilj-local-unit-{label}-{}",
            skilj_core::shared::generate_token_id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("data.pgpass")
    }

    #[test]
    fn a_first_start_generates_a_random_password() {
        let first = load_or_create_password(&temp_path("a")).unwrap();
        let second = load_or_create_password(&temp_path("b")).unwrap();
        assert_eq!(first.len(), 64, "256 bits, hex-encoded");
        assert_ne!(first, second);
    }

    #[test]
    fn a_later_start_reads_the_same_password_back() {
        let path = temp_path("reuse");
        let created = load_or_create_password(&path).unwrap();
        assert_eq!(load_or_create_password(&path).unwrap(), created);
    }

    #[cfg(unix)]
    #[test]
    fn the_password_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let path = temp_path("mode");
        load_or_create_password(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn an_existing_world_readable_file_is_narrowed() {
        use std::os::unix::fs::PermissionsExt;
        let path = temp_path("narrow");
        std::fs::write(&path, "existing-password\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(load_or_create_password(&path).unwrap(), "existing-password");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn an_empty_password_file_is_refused() {
        let path = temp_path("empty");
        std::fs::write(&path, "\n").unwrap();
        assert!(matches!(
            load_or_create_password(&path),
            Err(Error::EmptyPasswordFile { .. })
        ));
    }

    #[test]
    fn a_data_directory_with_trust_logins_is_refused() {
        let dir = temp_path("hba").with_extension("");
        std::fs::create_dir_all(&dir).unwrap();
        let hba = dir.join("pg_hba.conf");
        std::fs::write(&hba, "# trust is mentioned in a comment only\nlocal all all peer\nhost all all 127.0.0.1/32 scram-sha-256\n").unwrap();
        assert!(refuse_trust_authentication(&dir).is_ok());
        std::fs::write(&hba, "host all all 127.0.0.1/32 trust\n").unwrap();
        assert!(matches!(
            refuse_trust_authentication(&dir),
            Err(Error::TrustAuthentication { .. })
        ));
    }

    #[test]
    fn the_version_is_pinned() {
        let version = postgres_version();
        assert!(version.matches(&postgresql_embedded::Version::new(18, 6, 0)));
        // 18.6.1 drops its bundled UTC timezone on extraction (upstream #263).
        assert!(!version.matches(&postgresql_embedded::Version::new(18, 6, 1)));
        assert!(!version.matches(&postgresql_embedded::Version::new(19, 0, 0)));
    }
}
