//! What starting this release would change in a database, found without
//! changing it, and the schema compatibility level releases record
//! (docs/architecture.md §249, docs/upgrade-preflight-design.md).
//!
//! [`inspect`] needs only the database: the global migrations, the
//! compatibility level, the admin context's access trail, and every
//! bounded context's schema against the shape this release gives it.
//! Every read runs in one `READ ONLY` transaction, which Postgres
//! enforces, so the report is a consistent snapshot and a check cannot
//! write. What depends on the application's registered types, key provider
//! or blind-index key is checked by `skilj`'s `SkiljBuilder::preflight`,
//! which adds to the same report.
//!
//! The report holds names, counts, codes and versions: no payloads, tag
//! values, key material or connection details.

use crate::db::Pool;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// This release's schema level: raised by every release that changes the
/// schema, global or per bounded context.
pub const SCHEMA_LEVEL: i64 = 1;

/// The oldest schema level whose binaries may keep running against a
/// database this release has upgraded. Equal to [`SCHEMA_LEVEL`] unless a
/// release pair has been declared compatible and tested
/// (docs/upgrade-preflight-design.md, "Supported upgrades").
pub const MIN_COMPATIBLE_SCHEMA_LEVEL: i64 = 1;

/// The version of [`PreflightReport`]'s own layout.
pub const REPORT_FORMAT: u32 = 1;

/// Options for [`inspect`].
#[derive(Debug, Clone, Default)]
pub struct InspectOptions {
    pub(crate) exact_counts: bool,
}

impl InspectOptions {
    /// The defaults: row counts are the planner's estimates, which read no
    /// table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts the rows of every table a patch would lock or rewrite with
    /// `count(*)`, instead of estimating. Scans those tables.
    pub fn exact_counts(mut self, exact: bool) -> Self {
        self.exact_counts = exact;
        self
    }
}

/// Refuses with `schema_too_new` when the database was upgraded by a
/// release that doesn't declare this one compatible, as startup does
/// before anything else (docs/architecture.md §249). Reads only.
pub async fn check_compatibility(pool: &Pool) -> crate::error::Result<()> {
    crate::db::check_schema_compatibility(pool).await
}

/// Inspects the database `pool` connects to, changing nothing.
pub async fn inspect(
    pool: &Pool,
    options: &InspectOptions,
) -> crate::error::Result<PreflightReport> {
    crate::db::inspect_for_preflight(pool, options).await
}

/// Options for [`migrate`].
#[derive(Debug, Clone, Default)]
pub struct MigrateOptions {
    pub(crate) lock_timeout: Option<Duration>,
}

impl MigrateOptions {
    /// The defaults: no lock timeout, so a change waits as long as it
    /// takes for its table lock, as at startup.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gives up on a lock not granted within `timeout` (Postgres's
    /// `lock_timeout`, on every connection the migration uses). The step
    /// waiting for it fails and so does [`migrate`]; every step finished
    /// before it stays done, and running [`migrate`] again carries on from
    /// there. It also applies to the advisory locks that serialize
    /// concurrent migrations, so a second migration running at the same
    /// time can fail the same way.
    pub fn lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = Some(timeout);
        self
    }
}

/// What [`migrate`] found before it started and after it finished.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MigrationReport {
    /// The preflight before any change.
    pub before: PreflightReport,
    /// The preflight after: [`Verdict::UpToDate`] unless something changed
    /// the schema meanwhile.
    pub after: PreflightReport,
}

/// Upgrades the database's schema to this release's, and does nothing
/// else: the global migrations, the admin bounded context and its access
/// trail, and every bounded context's schema patches - what
/// `Skilj::build()` does before it reconciles registrations, without
/// registering anything, starting a background task or contacting a key
/// provider. Then records this release's schema level. For a deploy job
/// that owns the schema change; application instances can then start with
/// `skilj`'s `SchemaChanges::Refuse` (docs/architecture.md §249).
///
/// Refuses, before changing anything, when the database was upgraded by a
/// release that doesn't declare this one compatible (`schema_too_new`).
/// Safe to run on several processes at once: each step serializes on its
/// own lock and skips what another already did.
pub async fn migrate(
    database_url: &str,
    options: &MigrateOptions,
) -> crate::error::Result<MigrationReport> {
    let mut connect: sqlx::postgres::PgConnectOptions = database_url.parse()?;
    if let Some(timeout) = options.lock_timeout {
        connect = connect.options([("lock_timeout", format!("{}ms", timeout.as_millis().max(1)))]);
    }
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(connect)
        .await?;
    let report = migrate_pool(&pool).await;
    pool.close().await;
    report
}

async fn migrate_pool(pool: &Pool) -> crate::error::Result<MigrationReport> {
    let before = inspect(pool, &InspectOptions::new()).await?;
    crate::db::migrate(pool).await?;
    crate::db::ensure_admin_bounded_context(pool).await?;
    for bc in crate::db::list_bounded_contexts(pool).await? {
        if let Err(err) = crate::db::upgrade_bounded_context(pool, &bc).await {
            // Deleted since the listing (docs/architecture.md §157).
            if crate::db::get_bounded_context(pool, &bc.name)
                .await?
                .is_some()
            {
                return Err(err);
            }
        }
    }
    crate::db::ensure_access_trail(pool).await?;
    crate::db::record_schema_level(pool).await?;
    let after = inspect(pool, &InspectOptions::new()).await?;
    Ok(MigrationReport { before, after })
}

/// What starting this release would change, refuse or leave undetermined.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PreflightReport {
    /// [`REPORT_FORMAT`] at the time of writing.
    pub format: u32,
    /// The release that wrote the report.
    pub skilj_version: String,
    /// The database server's major version.
    pub server_major: u32,
    /// The conclusion.
    pub verdict: Verdict,
    /// Why this release would refuse to start, if it would.
    pub refusals: Vec<Refusal>,
    /// This binary's schema level, and the one recorded in the database.
    pub compatibility: Compatibility,
    /// The global migrations.
    pub global: GlobalMigrations,
    /// Whether the database's epoch changed since an instance last ran.
    pub epoch: EpochState,
    /// The admin bounded context's access trail.
    pub admin_context: AdminContext,
    /// Every bounded context's schema.
    pub bounded_contexts: Vec<BoundedContextReport>,
    /// What reconciliation would do with the application's registered
    /// types. Empty from [`inspect`]; filled by `SkiljBuilder::preflight`.
    pub registrations: Vec<RegistrationOutcome>,
    /// Checks that could not be answered, and why.
    pub unknown: Vec<Unknown>,
}

impl PreflightReport {
    /// Whether startup would change the schema: [`Verdict::UpgradeNeeded`].
    pub fn upgrade_needed(&self) -> bool {
        self.verdict == Verdict::UpgradeNeeded
    }

    /// One line: the refusals, then what an upgrade would change. The
    /// `Display` form has the detail.
    pub fn summary(&self) -> String {
        let mut parts: Vec<String> = self
            .refusals
            .iter()
            .map(|r| format!("refused: {}", r.code))
            .collect();
        if !self.global.pending.is_empty() {
            parts.push(format!(
                "{} global migration(s) pending",
                self.global.pending.len()
            ));
        }
        if self
            .compatibility
            .recorded
            .as_ref()
            .is_none_or(|r| r.level < self.compatibility.binary_level)
        {
            parts.push(format!(
                "schema level {} not recorded yet",
                self.compatibility.binary_level
            ));
        }
        let admin = &self.admin_context;
        if !admin.exists {
            parts.push("admin bounded context missing".to_string());
        } else if !admin.origin_kind_widened || !admin.event_types_to_register.is_empty() {
            parts.push("admin access trail to set up".to_string());
        }
        let patched: Vec<&str> = self
            .bounded_contexts
            .iter()
            .filter(|bc| bc.needs_upgrade())
            .map(|bc| bc.name.as_str())
            .collect();
        if !patched.is_empty() {
            parts.push(format!(
                "{} bounded context(s) to patch: {}",
                patched.len(),
                patched.join(", ")
            ));
        }
        if parts.is_empty() {
            "nothing".to_string()
        } else {
            parts.join("; ")
        }
    }
}

/// A preflight's conclusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Verdict {
    /// Starting changes no schema.
    UpToDate,
    /// Starting upgrades the schema; the report says what.
    UpgradeNeeded,
    /// Starting would fail; [`PreflightReport::refusals`] says why.
    Refused,
}

/// A reason startup would fail.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Refusal {
    /// The error code startup would fail with, such as `schema_too_new`.
    pub code: String,
    /// What was found.
    pub detail: String,
}

/// Schema levels: this binary's, and the database's.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Compatibility {
    /// [`SCHEMA_LEVEL`].
    pub binary_level: i64,
    /// [`MIN_COMPATIBLE_SCHEMA_LEVEL`].
    pub binary_min_compatible_level: i64,
    /// What the release that last upgraded the database recorded; `None`
    /// before any release recording one has finished an upgrade.
    pub recorded: Option<RecordedLevel>,
}

/// The schema level recorded in the database.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RecordedLevel {
    /// The schema's level.
    pub level: i64,
    /// The oldest level whose binaries may run against it.
    pub min_compatible_level: i64,
    /// The release that recorded it.
    pub upgraded_by: String,
    /// When.
    pub upgraded_at: DateTime<Utc>,
}

/// The global migrations, compared with the ones this binary embeds.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct GlobalMigrations {
    /// Applied, and identical to this binary's.
    pub applied: Vec<i64>,
    /// Not applied yet.
    pub pending: Vec<PendingMigration>,
    /// Applied by a newer release; this binary doesn't know them.
    pub unknown: Vec<i64>,
    /// Applied, but different from this binary's.
    pub checksum_mismatch: Vec<i64>,
    /// Recorded as failed.
    pub failed: Vec<i64>,
}

/// A global migration startup would apply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PendingMigration {
    /// Its number.
    pub version: i64,
    /// Its description.
    pub description: String,
}

/// The database epoch (docs/architecture.md §176).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EpochState {
    /// An instance has recorded one.
    pub recorded: bool,
    /// The recorded one isn't the current one: a promotion or restore
    /// since, which startup reports.
    pub changed: bool,
}

/// The admin bounded context and its access trail (docs/architecture.md
/// §229).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AdminContext {
    /// It exists; startup creates it otherwise.
    pub exists: bool,
    /// Its events accept library-recorded events.
    pub origin_kind_widened: bool,
    /// Access-trail and master-key event types startup registers or
    /// updates.
    pub event_types_to_register: Vec<String>,
    /// Active grants with no history, which startup records a baseline
    /// event for. Upkeep rather than an upgrade: it doesn't make the
    /// verdict [`Verdict::UpgradeNeeded`]. `None` when it can't be read
    /// yet.
    pub grants_to_baseline: Option<u64>,
}

/// One bounded context's schema against the shape this release gives it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BoundedContextReport {
    /// The bounded context.
    pub name: String,
    /// It is registered but its schema doesn't exist.
    pub schema_missing: bool,
    /// Tables, columns, indexes, constraints, triggers and functions
    /// startup adds.
    pub missing: Vec<String>,
    /// Functions whose body startup replaces.
    pub outdated: Vec<String>,
    /// Objects this release doesn't know: a newer release's, or a manual
    /// change. Startup leaves them alone.
    pub unexpected: Vec<String>,
    /// Objects of an older release that startup removes.
    pub drops: Vec<String>,
    /// Columns added on first use rather than at startup.
    pub lazy: Vec<String>,
    /// Patches that rewrite or delete rows, or lock writes out while they
    /// work through a table.
    pub rewrites: Vec<Rewrite>,
    /// The table locks the missing columns, indexes and constraints take.
    pub locks: Vec<TableLock>,
}

impl BoundedContextReport {
    /// Whether startup would change this bounded context's schema.
    pub fn needs_upgrade(&self) -> bool {
        !self.missing.is_empty()
            || !self.outdated.is_empty()
            || !self.drops.is_empty()
            || !self.rewrites.is_empty()
    }
}

/// A patch that rewrites rows or holds a write-blocking lock for a while.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Rewrite {
    /// Which: `tag_dictionary`, `source_content_scrub`,
    /// `parked_delivery_dedupe` or `idempotency_key_rescope`.
    pub name: String,
    /// The table it works through.
    pub table: String,
    /// The lock it holds on it.
    pub lock: String,
    /// What waits meanwhile.
    pub blocks: String,
    /// How many rows the table holds.
    pub rows: RowCount,
}

/// The lock adding missing objects to one table takes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TableLock {
    /// The table.
    pub table: String,
    /// The strongest lock taken: `ACCESS EXCLUSIVE` (columns, constraints;
    /// brief, except a constraint validating every row) or `SHARE`
    /// (indexes; blocks writes while the index builds).
    pub mode: String,
    /// What is added.
    pub reason: String,
    /// How many rows the table holds.
    pub rows: RowCount,
}

/// A table's row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RowCount {
    /// The count; `None` when the table has never been analyzed and no
    /// exact count was asked for.
    pub value: Option<i64>,
    /// Counted rather than estimated.
    pub exact: bool,
}

impl Refusal {
    /// A refusal.
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

/// What reconciliation would do with one registered type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RegistrationOutcome {
    /// Its bounded context.
    pub bounded_context: String,
    /// `event_type`, `command_type`, `projection` or `blind_tag`.
    pub kind: String,
    /// Its name (a blind tag's tag key).
    pub name: String,
    /// `registered` (new), `updated`, `unchanged`, `kept_newer`,
    /// `kept_protections`, `rebuild_staged`, `rotated`, `skipped_no_access`
    /// or `refused`.
    pub outcome: String,
    /// The refusal's code, or the history a staged rebuild replays.
    pub detail: Option<String>,
}

impl RegistrationOutcome {
    /// An outcome.
    pub fn new(
        bounded_context: impl Into<String>,
        kind: impl Into<String>,
        name: impl Into<String>,
        outcome: impl Into<String>,
        detail: Option<String>,
    ) -> Self {
        Self {
            bounded_context: bounded_context.into(),
            kind: kind.into(),
            name: name.into(),
            outcome: outcome.into(),
            detail,
        }
    }
}

/// A check the preflight could not answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Unknown {
    /// Which check.
    pub check: String,
    /// Why not.
    pub reason: String,
}

impl Unknown {
    /// An unanswered check.
    pub fn new(check: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            check: check.into(),
            reason: reason.into(),
        }
    }
}

fn rows(count: &RowCount) -> String {
    match (count.value, count.exact) {
        (Some(n), true) => format!("{n} rows"),
        (Some(n), false) => format!("~{n} rows"),
        (None, _) => "rows unknown (never analyzed)".to_string(),
    }
}

impl fmt::Display for PreflightReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verdict = match self.verdict {
            Verdict::UpToDate => "up to date",
            Verdict::UpgradeNeeded => "upgrade needed",
            Verdict::Refused => "refused",
        };
        writeln!(
            f,
            "skilj {} preflight (PostgreSQL {}): {verdict}",
            self.skilj_version, self.server_major
        )?;
        for refusal in &self.refusals {
            writeln!(f, "  refused: {} - {}", refusal.code, refusal.detail)?;
        }
        match &self.compatibility.recorded {
            Some(r) => writeln!(
                f,
                "  schema level {} (oldest compatible {}), recorded by {}; this release: {}",
                r.level, r.min_compatible_level, r.upgraded_by, self.compatibility.binary_level
            )?,
            None => writeln!(
                f,
                "  no schema level recorded; this release records {}",
                self.compatibility.binary_level
            )?,
        }
        for m in &self.global.pending {
            writeln!(
                f,
                "  global migration {} pending: {}",
                m.version, m.description
            )?;
        }
        if !self.global.unknown.is_empty() {
            writeln!(
                f,
                "  global migrations from a newer release: {:?}",
                self.global.unknown
            )?;
        }
        if self.epoch.changed {
            writeln!(f, "  the database epoch changed since an instance last ran")?;
        }
        let admin = &self.admin_context;
        if !admin.exists {
            writeln!(f, "  admin bounded context: created at startup")?;
        } else {
            if !admin.origin_kind_widened {
                writeln!(f, "  admin events: origin_kind check widened")?;
            }
            if !admin.event_types_to_register.is_empty() {
                writeln!(
                    f,
                    "  admin event types registered: {}",
                    admin.event_types_to_register.join(", ")
                )?;
            }
            match admin.grants_to_baseline {
                Some(0) => {}
                Some(n) => writeln!(f, "  access trail: {n} grants get a baseline event")?,
                None => {}
            }
        }
        for bc in &self.bounded_contexts {
            if !bc.needs_upgrade() && bc.unexpected.is_empty() && !bc.schema_missing {
                continue;
            }
            writeln!(f, "  bounded context {}:", bc.name)?;
            if bc.schema_missing {
                writeln!(f, "    schema missing")?;
            }
            for line in &bc.missing {
                writeln!(f, "    adds {line}")?;
            }
            for line in &bc.outdated {
                writeln!(f, "    replaces {line}")?;
            }
            for line in &bc.drops {
                writeln!(f, "    drops {line}")?;
            }
            for line in &bc.unexpected {
                writeln!(f, "    not known to this release: {line}")?;
            }
            for r in &bc.rewrites {
                writeln!(
                    f,
                    "    {} on {} ({}): {} lock, blocks {}",
                    r.name,
                    r.table,
                    rows(&r.rows),
                    r.lock,
                    r.blocks
                )?;
            }
            for l in &bc.locks {
                writeln!(
                    f,
                    "    {} lock on {} ({}): {}",
                    l.mode,
                    l.table,
                    rows(&l.rows),
                    l.reason
                )?;
            }
        }
        for r in &self.registrations {
            if r.outcome == "unchanged" {
                continue;
            }
            write!(
                f,
                "  {} {}/{}: {}",
                r.kind, r.bounded_context, r.name, r.outcome
            )?;
            match &r.detail {
                Some(detail) => writeln!(f, " ({detail})")?,
                None => writeln!(f)?,
            }
        }
        for u in &self.unknown {
            writeln!(f, "  not checked: {} - {}", u.check, u.reason)?;
        }
        Ok(())
    }
}
